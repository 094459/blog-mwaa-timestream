from aws_cdk import (
    aws_iam as iam,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_s3 as s3,
    aws_sns_subscriptions as subs,
    aws_timestream as timestream,
    core
)


class AmazonTimestreamAirflowStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, ts_props, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        timedb = timestream.CfnDatabase(
            self,
            "TimestreamDB",
            database_name=f"{ts_props['timestream_db'].lower()}"
            #kms_key_id={string of kms_key_id if you want to use this}
            )
        
        tstable = timestream.CfnTable(
            self,
            "TimestreamDBTable",
            database_name=f"{ts_props['timestream_db'].lower()}",
            table_name=f"{ts_props['timestream_table'].lower()}",
            retention_properties= {"MemoryStoreRetentionPeriodInHours": "24","MagneticStoreRetentionPeriodInDays": "1"}
        )

        tstable.node.add_dependency(timedb)

        # create an IAM policy which we can attach to MWAA that provides
        # access to a specific Timestream database and table and a specific
        # Amazon S3 bucket

        # Grab arn of our S3 bucket - this should already exist. If not this will fail

        s3_export_bucket = s3.Bucket.from_bucket_name(self,"DataLakeExport",bucket_name=f"{ts_props['s3_export']}")
        s3_export_arn = s3_export_bucket.bucket_arn

        timestream_mwaa_policy = iam.ManagedPolicy(
            self,
            "TimeStreamPolicy",
            managed_policy_name="TimeStreamPolicyforMWAAIntegration",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "s3:GetBucket*",
                        "s3:List*",
                        "s3:GetObject*"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"{s3_export_arn}/*",
                        f"{s3_export_arn}"
                        ],
                ),
                iam.PolicyStatement(
                    actions=[
                        "timestream:Select",
                        "timestream:DescribeTable",
                        "timestream:ListTables"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[f"arn:aws:timestream:{self.region}:{self.account}:database/{ts_props['timestream_db']}/table/{ts_props['timestream_table']}*"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "timestream:DescribeEndpoints",
                        "timestream:SelectValues",
                        "timestream:CancelQuery"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=["*"],
                ),
            ]
        )

        core.CfnOutput(
            self,
            id="MWAATimestreamIAMPolicy",
            value=timestream_mwaa_policy.managed_policy_arn,
            description="MWAA to Timestream IAM policy arn"
        )

        



