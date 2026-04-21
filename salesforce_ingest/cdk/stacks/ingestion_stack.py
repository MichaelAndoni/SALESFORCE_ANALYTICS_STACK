# cdk/stacks/ingestion_stack.py
#
# Defines all AWS infrastructure for the Salesforce → S3 → Snowpipe pipeline:
#
#   • S3 landing bucket  (versioned, lifecycle-tiered, encrypted)
#   • SQS queue          (S3 event → Snowpipe notification)
#   • Lambda extractor   (scheduled via EventBridge)
#   • Lambda pipe monitor (health checks)
#   • SSM parameters     (initial watermarks)
#   • CloudWatch alarms  (pipe stalls, Lambda errors, DLQ depth)
#   • IAM roles          (least-privilege throughout)
#   • Secrets Manager    (Salesforce PEM key reference)

from __future__ import annotations

from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_cloudwatch         as cw,
    aws_cloudwatch_actions as cw_actions,
    aws_events             as events,
    aws_events_targets     as targets,
    aws_iam                as iam,
    aws_lambda             as lambda_,
    aws_lambda_event_sources as lambda_es,
    aws_logs               as logs,
    aws_s3                 as s3,
    aws_s3_notifications   as s3n,
    aws_secretsmanager     as sm,
    aws_sns                as sns,
    aws_sqs                as sqs,
    aws_ssm                as ssm,
)
from constructs import Construct

# All Salesforce objects the extractor handles
SALESFORCE_OBJECTS = [
    "Account", "Contact", "Lead", "Opportunity",
    "OpportunityLineItem", "Campaign", "CampaignMember",
    "Task", "Event", "User",
]


class SalesforceIngestionStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        env_name: str = "dev",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.env_name = env_name
        prefix        = f"sf-ingest-{env_name}"

        # ── SNS topic for all alarms ──────────────────────────────────────
        alarm_topic = sns.Topic(
            self, "AlarmTopic",
            topic_name=f"{prefix}-alarms",
            display_name="Salesforce Ingest Alarms",
        )

        # ── S3 landing bucket ─────────────────────────────────────────────
        landing_bucket = s3.Bucket(
            self, "LandingBucket",
            bucket_name=f"{prefix}-landing-{self.account}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.RETAIN,   # Never delete raw data
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="TransitionToIA",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(30),
                        ),
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(90),
                        ),
                    ],
                )
            ],
        )

        # ── SQS queue for Snowpipe event notifications ────────────────────
        # Snowpipe polls this queue to discover new files in S3.
        snowpipe_dlq = sqs.Queue(
            self, "SnowpipeDLQ",
            queue_name=f"{prefix}-snowpipe-dlq",
            retention_period=Duration.days(14),
        )

        snowpipe_queue = sqs.Queue(
            self, "SnowpipeQueue",
            queue_name=f"{prefix}-snowpipe-notifications",
            visibility_timeout=Duration.seconds(300),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=snowpipe_dlq,
            ),
        )

        # Grant S3 permission to send messages to the queue
        snowpipe_queue.add_to_resource_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                principals=[iam.ServicePrincipal("s3.amazonaws.com")],
                actions=["sqs:SendMessage"],
                resources=[snowpipe_queue.queue_arn],
                conditions={
                    "ArnLike": {
                        "aws:SourceArn": landing_bucket.bucket_arn
                    }
                },
            )
        )

        # Attach S3 event notification — fires for every new JSON file
        landing_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(snowpipe_queue),
            s3.NotificationKeyFilter(suffix=".json"),
        )

        # ── IAM role for Snowflake (Snowpipe storage integration) ─────────
        # Snowflake assumes this role to read from the landing bucket and
        # receive messages from the SQS queue.
        # After deploying, paste the role ARN into the Snowflake storage
        # integration (see snowflake/01_raw_schema_setup.sql).
        snowflake_role = iam.Role(
            self, "SnowflakeRole",
            role_name=f"{prefix}-snowflake-access",
            # The trust policy is updated post-deploy with Snowflake's
            # IAM user ARN and external ID from DESCRIBE INTEGRATION output.
            # Placeholder trust here; tighten after integration is created.
            assumed_by=iam.AccountRootPrincipal(),
            description="Assumed by Snowflake Snowpipe storage integration",
        )
        landing_bucket.grant_read(snowflake_role)
        snowpipe_queue.grant_consume_messages(snowflake_role)

        # ── Secrets Manager: Salesforce private key ───────────────────────
        # Create the secret shell here; populate the PEM value manually or
        # via CI/CD after generating the Connected App certificate.
        sf_secret = sm.Secret(
            self, "SalesforceSecret",
            secret_name=f"/{prefix}/salesforce-credentials",
            description=(
                "Salesforce Connected App credentials for JWT Bearer auth. "
                "JSON: {private_key_pem: string}"
            ),
            generate_secret_string=sm.SecretStringGenerator(
                secret_string_template='{"private_key_pem": "PLACEHOLDER"}',
                generate_string_key="__unused__",
                exclude_characters=" ",
            ),
        )

        # ── Secrets Manager: Snowflake credentials (for pipe monitor) ─────
        sf_snow_secret = sm.Secret(
            self, "SnowflakeSecret",
            secret_name=f"/{prefix}/snowflake-credentials",
            description=(
                "Snowflake key-pair credentials for pipe monitor Lambda. "
                "JSON: {private_key_der: base64-encoded DER bytes}"
            ),
        )

        # ── SSM: initial watermarks (will be overwritten by Lambda) ───────
        ssm_prefix = f"/salesforce-ingest/{env_name}/watermarks"
        for obj in SALESFORCE_OBJECTS:
            ssm.StringParameter(
                self, f"Watermark{obj}",
                parameter_name=f"{ssm_prefix}/{obj}",
                string_value="2020-01-01T00:00:00+00:00",
                description=f"Salesforce incremental watermark for {obj}",
            )

        # ── Lambda: extractor ─────────────────────────────────────────────
        extractor_role = iam.Role(
            self, "ExtractorRole",
            role_name=f"{prefix}-extractor-lambda",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )

        # Least-privilege S3 writes to the landing bucket
        extractor_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:PutObject"],
                resources=[f"{landing_bucket.bucket_arn}/salesforce/*"],
            )
        )
        # SSM read/write for watermarks only
        extractor_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "ssm:GetParameter",
                    "ssm:GetParameters",
                    "ssm:PutParameter",
                ],
                resources=[
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter"
                    f"/salesforce-ingest/{env_name}/watermarks/*"
                ],
            )
        )
        # Secrets Manager read for SF credentials
        sf_secret.grant_read(extractor_role)

        extractor_fn = lambda_.Function(
            self, "ExtractorFunction",
            function_name=f"{prefix}-extractor",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset(
                "../lambda/extractor",
                bundling={
                    "image": lambda_.Runtime.PYTHON_3_12.bundling_image,
                    "command": [
                        "bash", "-c",
                        "pip install -r requirements.txt -t /asset-output && "
                        "cp -r . /asset-output",
                    ],
                },
            ),
            role=extractor_role,
            timeout=Duration.minutes(15),    # Bulk API jobs can take several minutes
            memory_size=1024,                # Enough for streaming large CSV pages
            environment={
                "SALESFORCE_SECRET_NAME": sf_secret.secret_name,
                "SSM_WATERMARK_PREFIX":   ssm_prefix,
                "S3_BUCKET":              landing_bucket.bucket_name,
                "S3_PREFIX":              "salesforce/raw",
                "SF_INSTANCE_URL":        self.node.try_get_context("sf_instance_url") or "REPLACE_ME",
                "SF_USERNAME":            self.node.try_get_context("sf_username")     or "REPLACE_ME",
                "SF_CLIENT_ID":           self.node.try_get_context("sf_client_id")    or "REPLACE_ME",
                "SF_SANDBOX":             "true" if env_name != "prod" else "false",
                "LOOKBACK_MINUTES":       "30",
                "OBJECTS":                ",".join(SALESFORCE_OBJECTS),
            },
            log_retention=logs.RetentionDays.ONE_MONTH,
        )

        # EventBridge schedule: every hour, on the hour
        events.Rule(
            self, "ExtractorSchedule",
            rule_name=f"{prefix}-extractor-schedule",
            schedule=events.Schedule.cron(
                minute="0",     # top of every hour
                hour="*",
                day="*",
                month="*",
                year="*",
            ),
            targets=[
                targets.LambdaFunction(
                    extractor_fn,
                    retry_attempts=2,
                )
            ],
        )

        # CloudWatch alarm: Lambda errors
        cw.Alarm(
            self, "ExtractorErrorAlarm",
            alarm_name=f"{prefix}-extractor-errors",
            alarm_description="Salesforce extractor Lambda is failing",
            metric=extractor_fn.metric_errors(
                period=Duration.minutes(15),
                statistic="Sum",
            ),
            threshold=1,
            evaluation_periods=1,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(cw_actions.SnsAction(alarm_topic))

        # CloudWatch alarm: SQS DLQ depth (means S3 notifications are failing)
        cw.Alarm(
            self, "SnowpipeDLQAlarm",
            alarm_name=f"{prefix}-snowpipe-dlq-depth",
            alarm_description="Messages in Snowpipe DLQ — SQS notifications failing",
            metric=snowpipe_dlq.metric_approximate_number_of_messages_visible(
                period=Duration.minutes(5),
            ),
            threshold=1,
            evaluation_periods=1,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(cw_actions.SnsAction(alarm_topic))

        # ── Lambda: pipe monitor ──────────────────────────────────────────
        monitor_role = iam.Role(
            self, "MonitorRole",
            role_name=f"{prefix}-monitor-lambda",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        monitor_role.add_to_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
            )
        )
        sf_snow_secret.grant_read(monitor_role)

        monitor_fn = lambda_.Function(
            self, "MonitorFunction",
            function_name=f"{prefix}-pipe-monitor",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset(
                "../lambda/pipe_monitor",
                bundling={
                    "image": lambda_.Runtime.PYTHON_3_12.bundling_image,
                    "command": [
                        "bash", "-c",
                        "pip install snowflake-connector-python -t /asset-output && "
                        "cp -r . /asset-output",
                    ],
                },
            ),
            role=monitor_role,
            timeout=Duration.minutes(5),
            memory_size=256,
            environment={
                "SNOWFLAKE_SECRET_NAME": sf_snow_secret.secret_name,
                "SNOWFLAKE_ACCOUNT":     self.node.try_get_context("snowflake_account") or "REPLACE_ME",
                "SNOWFLAKE_USER":        self.node.try_get_context("snowflake_user")    or "REPLACE_ME",
                "SNOWFLAKE_ROLE":        "LOADER",
                "SNOWFLAKE_WAREHOUSE":   "LOADING",
                "RAW_DATABASE":          "RAW",
                "RAW_SCHEMA":            "SALESFORCE",
                "CW_METRIC_NAMESPACE":   "SalesforceIngest",
            },
            log_retention=logs.RetentionDays.ONE_WEEK,
        )

        events.Rule(
            self, "MonitorSchedule",
            rule_name=f"{prefix}-monitor-schedule",
            schedule=events.Schedule.rate(Duration.minutes(15)),
            targets=[targets.LambdaFunction(monitor_fn)],
        )

        # ── Outputs ───────────────────────────────────────────────────────
        CfnOutput(self, "LandingBucketName",
                  value=landing_bucket.bucket_name,
                  description="S3 landing bucket — paste into Snowflake stage definition")

        CfnOutput(self, "SnowpipeQueueUrl",
                  value=snowpipe_queue.queue_url,
                  description="SQS URL — paste into CREATE PIPE ... ERROR_INTEGRATION or use storage integration")

        CfnOutput(self, "SnowpipeQueueArn",
                  value=snowpipe_queue.queue_arn,
                  description="SQS ARN — paste into Snowflake storage integration")

        CfnOutput(self, "SnowflakeRoleArn",
                  value=snowflake_role.role_arn,
                  description="IAM role Snowflake will assume — paste into CREATE STORAGE INTEGRATION")

        CfnOutput(self, "SalesforceSecretArn",
                  value=sf_secret.secret_arn,
                  description="Secrets Manager ARN for Salesforce PEM key")

        CfnOutput(self, "AlarmTopicArn",
                  value=alarm_topic.topic_arn,
                  description="SNS topic ARN — subscribe your email/PagerDuty here")
