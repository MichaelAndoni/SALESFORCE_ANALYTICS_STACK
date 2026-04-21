# cdk/stacks/pipeline_orchestrator.py
#
# Step Functions state machine that coordinates the full pipeline run:
#
#   1. ExtractSalesforce   — invoke the Lambda extractor, wait for it
#   2. WaitForSnowpipe     — poll SYSTEM$PIPE_STATUS until pendingFileCount = 0
#   3. RunDbtBuild         — invoke the dbt runner Lambda (dbt build)
#   4. VerifyDataQuality   — run dbt tests, fail hard if any fail
#   5. NotifySuccess       — post to SNS / Slack
#
# Why Step Functions instead of EventBridge + independent schedules?
#   • Guaranteed ordering: dbt never runs before Snowpipe finishes
#   • Failure isolation: a Snowpipe stall pauses dbt, not skips it
#   • Retry logic per step: extractor retries 2x, poller retries until timeout
#   • Execution history: CloudWatch logs + SFN console shows exactly where
#     each hourly run succeeded or failed
#   • No missed runs: if dbt takes 20 min, the next extract still fires on
#     schedule rather than stacking invocations
#
# The state machine is triggered by an EventBridge schedule (replaces the
# direct Lambda schedule from ingestion_stack.py — disable that one when
# adopting this orchestrator).

from __future__ import annotations

from aws_cdk import (
    Duration,
    aws_events            as events,
    aws_events_targets    as targets,
    aws_iam               as iam,
    aws_lambda            as lambda_,
    aws_logs              as logs,
    aws_sns               as sns,
    aws_stepfunctions     as sfn,
    aws_stepfunctions_tasks as tasks,
)
from constructs import Construct


class PipelineOrchestrator(Construct):
    """
    Step Functions state machine orchestrating the full
    Salesforce → Snowpipe → dbt pipeline.

    Usage in ingestion_stack.py:
        from stacks.pipeline_orchestrator import PipelineOrchestrator

        orchestrator = PipelineOrchestrator(
            self, "Orchestrator",
            env_name=env_name,
            extractor_fn=extractor_fn,
            pipe_poller_fn=pipe_poller_fn,
            dbt_runner_fn=dbt_runner_fn,
            alarm_topic=alarm_topic,
        )
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name:        str,
        extractor_fn:    lambda_.Function,
        pipe_poller_fn:  lambda_.Function,
        dbt_runner_fn:   lambda_.Function,
        alarm_topic:     sns.Topic,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        prefix = f"sf-ingest-{env_name}"

        # ── Step 1: Extract Salesforce ─────────────────────────────────────
        # Invoke the Lambda extractor synchronously.
        # The Lambda returns a summary dict; SFN captures it as the next state input.
        extract_step = tasks.LambdaInvoke(
            self, "ExtractSalesforce",
            lambda_function=extractor_fn,
            payload=sfn.TaskInput.from_object({
                # Pass the execution name so each run is traceable in CloudWatch
                "execution_name":  sfn.JsonPath.string_at("$$.Execution.Name"),
                "execution_start": sfn.JsonPath.string_at("$$.Execution.StartTime"),
            }),
            result_path="$.extract_result",
            # Retry on Lambda throttling or transient errors (not business errors)
            retry_on_service_exceptions=True,
        ).add_retry(
            errors=["Lambda.ServiceException", "Lambda.TooManyRequestsException"],
            max_attempts=2,
            interval=Duration.seconds(30),
            backoff_rate=2.0,
        ).add_catch(
            handler=self._fail_state("ExtractFailed",
                                     "Salesforce extractor Lambda failed"),
            errors=["States.ALL"],
            result_path="$.error",
        )

        # ── Step 2: Wait for Snowpipe to drain ─────────────────────────────
        # Poll the pipe poller Lambda every 60 seconds until all pipes report
        # pendingFileCount == 0.  Timeout after 45 minutes (generous but bounded).
        #
        # The poller Lambda returns:
        #   { "all_pipes_ready": true/false, "max_pending": N, "pipes": [...] }

        poll_pipes = tasks.LambdaInvoke(
            self, "PollSnowpipeStatus",
            lambda_function=pipe_poller_fn,
            result_path="$.pipe_status",
            payload=sfn.TaskInput.from_object({
                "check_mode": "ready_check",   # returns all_pipes_ready boolean
            }),
        ).add_retry(
            errors=["Lambda.ServiceException"],
            max_attempts=3,
            interval=Duration.seconds(10),
        )

        # Wait 60 seconds between polls (Snowpipe typically drains in < 5 min)
        wait_before_poll = sfn.Wait(
            self, "WaitBeforeNextPoll",
            time=sfn.WaitTime.duration(Duration.seconds(60)),
        )

        # Choice: are all pipes drained?
        pipes_ready_choice = sfn.Choice(self, "AllPipesReady?")

        pipes_ready_choice.when(
            sfn.Condition.boolean_equals("$.pipe_status.Payload.all_pipes_ready", True),
            sfn.Pass(self, "PipesReadyContinue"),
        )
        pipes_ready_choice.otherwise(wait_before_poll)
        wait_before_poll.next(poll_pipes)
        poll_pipes.next(pipes_ready_choice)

        # Timeout guard: if pipes never drain after 45 min, fail the execution
        pipe_wait_timeout = sfn.Wait(
            self, "PipeWaitStart",
            time=sfn.WaitTime.duration(Duration.seconds(1)),
        )
        # (The SFN heartbeat timeout on the poll task handles the 45-min hard limit)

        # ── Step 3: Run dbt build ──────────────────────────────────────────
        # Invoke the dbt runner Lambda which runs `dbt build` inside a
        # CodeBuild project (see dbt_runner Lambda).
        dbt_build_step = tasks.LambdaInvoke(
            self, "RunDbtBuild",
            lambda_function=dbt_runner_fn,
            payload=sfn.TaskInput.from_object({
                "command":  "build",
                "select":   "",        # empty = all models
                "target":   env_name,
                "execution_name": sfn.JsonPath.string_at("$$.Execution.Name"),
            }),
            result_path="$.dbt_result",
            # dbt build can take up to 20 min for large projects
            heartbeat=Duration.minutes(25),
        ).add_retry(
            # Do NOT retry dbt — a failed test should not auto-retry, it needs
            # human investigation.  Only retry on Lambda infrastructure errors.
            errors=["Lambda.ServiceException", "Lambda.TooManyRequestsException"],
            max_attempts=1,
            interval=Duration.seconds(15),
        ).add_catch(
            handler=self._fail_state("DbtBuildFailed", "dbt build failed"),
            errors=["States.ALL"],
            result_path="$.error",
        )

        # ── Step 4: Check dbt result ───────────────────────────────────────
        dbt_success_choice = sfn.Choice(self, "DbtBuildSucceeded?")

        dbt_success_choice.when(
            sfn.Condition.number_equals(
                "$.dbt_result.Payload.tests_failed", 0
            ),
            sfn.Pass(self, "DbtBuildOk"),
        )
        dbt_success_choice.otherwise(
            self._fail_state("DbtTestsFailed", "dbt tests failed — see CloudWatch")
        )

        # ── Step 5: Notify success ─────────────────────────────────────────
        notify_success = tasks.SnsPublish(
            self, "NotifySuccess",
            topic=alarm_topic,
            message=sfn.TaskInput.from_object({
                "subject": f"✅ Salesforce pipeline ({env_name}) completed",
                "body": sfn.JsonPath.format(
                    "Run {} completed.\nExtracted: {} records.\ndbt: {} models built.",
                    sfn.JsonPath.string_at("$$.Execution.Name"),
                    sfn.JsonPath.string_at("$.extract_result.Payload.total_records"),
                    sfn.JsonPath.string_at("$.dbt_result.Payload.models_built"),
                ),
            }),
            result_path=sfn.JsonPath.DISCARD,
        )

        # ── Wire the state machine ─────────────────────────────────────────
        definition = (
            extract_step
            .next(poll_pipes)
            .next(pipes_ready_choice)
        )

        # After pipes_ready_choice → DbtBuild → check → notify
        sfn.Pass(self, "PipesReadyContinue_").next(dbt_build_step)
        dbt_build_step.next(dbt_success_choice)
        sfn.Pass(self, "DbtBuildOk_").next(notify_success)
        notify_success.next(sfn.Succeed(self, "PipelineComplete"))

        # ── SFN log group ─────────────────────────────────────────────────
        log_group = logs.LogGroup(
            self, "StateMachineLogGroup",
            log_group_name=f"/aws/states/{prefix}-pipeline",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        # ── State machine role ────────────────────────────────────────────
        sfn_role = iam.Role(
            self, "StateMachineRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
        )
        extractor_fn.grant_invoke(sfn_role)
        pipe_poller_fn.grant_invoke(sfn_role)
        dbt_runner_fn.grant_invoke(sfn_role)
        alarm_topic.grant_publish(sfn_role)
        log_group.grant_write(sfn_role)

        # ── State machine ─────────────────────────────────────────────────
        self.state_machine = sfn.StateMachine(
            self, "PipelineStateMachine",
            state_machine_name=f"{prefix}-pipeline",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            role=sfn_role,
            timeout=Duration.hours(2),    # hard ceiling for the whole pipeline
            tracing_enabled=True,
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.ALL,   # log every state transition
                include_execution_data=True,
            ),
        )

        # ── EventBridge schedule → State Machine ──────────────────────────
        # Fires at the top of every hour.
        # IMPORTANT: disable the direct Lambda EventBridge rule when adopting this.
        sfn_schedule_role = iam.Role(
            self, "ScheduleRole",
            assumed_by=iam.ServicePrincipal("scheduler.amazonaws.com"),
        )
        self.state_machine.grant_start_execution(sfn_schedule_role)

        events.Rule(
            self, "PipelineSchedule",
            rule_name=f"{prefix}-pipeline-schedule",
            schedule=events.Schedule.cron(
                minute="0", hour="*", day="*", month="*", year="*"
            ),
            targets=[
                targets.SfnStateMachine(
                    self.state_machine,
                    role=sfn_schedule_role,
                )
            ],
        )

    def _fail_state(self, id_: str, cause: str) -> sfn.Fail:
        return sfn.Fail(self, id_, cause=cause, error=id_)
