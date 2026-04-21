# cdk/stacks/codebuild_construct.py
#
# Provisions the AWS CodeBuild project that actually runs dbt build.
# Called by the dbt runner Lambda via start_build().

from __future__ import annotations

from aws_cdk import (
    Duration,
    aws_codebuild      as cb,
    aws_iam            as iam,
    aws_logs           as logs,
    aws_s3             as s3,
    aws_secretsmanager as sm,
)
from constructs import Construct


class DbtCodeBuildProject(Construct):
    """
    CodeBuild project that runs dbt inside the AWS environment.

    The Lambda dbt_runner calls start_build() on this project and
    polls until completion.  The project:
      - Clones the dbt Git repo
      - Installs dbt-snowflake from pip (cached between builds)
      - Writes profiles.yml from Secrets Manager
      - Runs dbt seed + snapshot + build
      - Exports result metrics as CodeBuild exported variables
      - Uploads manifest.json to S3 for slim CI state comparison
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name:            str,
        snowflake_secret:    sm.Secret,
        artifacts_bucket:    s3.Bucket,
        dbt_repo_url:        str,
        dbt_branch:          str = "main",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        prefix = f"sf-ingest-{env_name}"

        # ── IAM role for CodeBuild ────────────────────────────────────────
        build_role = iam.Role(
            self, "BuildRole",
            role_name=f"{prefix}-codebuild-dbt",
            assumed_by=iam.ServicePrincipal("codebuild.amazonaws.com"),
        )

        # Read Snowflake credentials
        snowflake_secret.grant_read(build_role)

        # Read/write artifacts bucket (manifests, run_results)
        artifacts_bucket.grant_read_write(build_role)

        # CloudWatch Logs
        build_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["*"],
            )
        )

        # ── Log group ─────────────────────────────────────────────────────
        log_group = logs.LogGroup(
            self, "BuildLogGroup",
            log_group_name=f"/aws/codebuild/{prefix}-dbt",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        # ── CodeBuild project ─────────────────────────────────────────────
        self.project = cb.Project(
            self, "DbtBuildProject",
            project_name=f"{prefix}-dbt-runner",
            description=f"Runs dbt build for salesforce_dbt ({env_name})",
            role=build_role,

            # Source: clone from Git repo on each build
            source=cb.Source.git_hub(
                owner=dbt_repo_url.split("/")[-2],
                repo=dbt_repo_url.split("/")[-1].replace(".git", ""),
                branch_or_ref=dbt_branch,
                clone_depth=1,        # shallow clone for speed
            ),

            # Use buildspec from the repo
            build_spec=cb.BuildSpec.from_source_filename(
                "salesforce_ingest/codebuild/buildspec.yml"
            ),

            environment=cb.BuildEnvironment(
                build_image=cb.LinuxBuildImage.STANDARD_7_0,  # Python 3.12
                compute_type=cb.ComputeType.SMALL,             # 3 GB RAM, 2 vCPUs
                privileged=False,
                environment_variables={
                    "SNOWFLAKE_SECRET_ARN": cb.BuildEnvironmentVariable(
                        value=snowflake_secret.secret_arn,
                        type=cb.BuildEnvironmentVariableType.PLAINTEXT,
                    ),
                    "S3_ARTIFACTS_BUCKET": cb.BuildEnvironmentVariable(
                        value=artifacts_bucket.bucket_name,
                        type=cb.BuildEnvironmentVariableType.PLAINTEXT,
                    ),
                    "DBT_TARGET": cb.BuildEnvironmentVariable(
                        value=env_name,
                        type=cb.BuildEnvironmentVariableType.PLAINTEXT,
                    ),
                    # Defaults — overridden per-invocation by the Lambda
                    "DBT_COMMAND":       cb.BuildEnvironmentVariable(value="build"),
                    "DBT_SELECT":        cb.BuildEnvironmentVariable(value=""),
                    "DBT_FULL_REFRESH":  cb.BuildEnvironmentVariable(value="false"),
                    "SFN_EXECUTION":     cb.BuildEnvironmentVariable(value="manual"),
                },
            ),

            # Cache pip packages and dbt_packages between builds
            cache=cb.Cache.local(
                cb.LocalCacheMode.CUSTOM,
                cb.LocalCacheMode.SOURCE,
            ),

            timeout=Duration.minutes(45),

            logging=cb.LoggingOptions(
                cloud_watch=cb.CloudWatchLoggingOptions(
                    log_group=log_group,
                    enabled=True,
                )
            ),

            # Export variables so Lambda can read build results
            # (populated by the buildspec from run_results.json)
        )

        # Grant the dbt runner Lambda permission to start builds on this project
        self.project_name = self.project.project_name
        self.project_arn  = self.project.project_arn
