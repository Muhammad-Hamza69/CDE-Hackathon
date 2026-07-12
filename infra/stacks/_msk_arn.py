"""Shared helper for building MSK topic/group resource ARNs from a cluster
ARN that is an unresolved CDK cross-stack token at synth time.

A plain Python `cluster_arn.replace(":cluster/", ":topic/")` is a no-op on a
token placeholder string (there's no literal ":cluster/" text in
"${Token[TOKEN.42]}"), which silently produces an IAM policy scoped to the
*cluster* ARN instead of a topic/group ARN - confirmed via CloudTrail
(TopicAuthorizationException at runtime despite the policy "looking" correct
in the CDK source). `Arn.split`/`Arn.format` were tried next, but CDK drops
the `resource_name` component when `resource` is a literal string and
`resource_name` is itself a token (verified directly against the resolved
CloudFormation template - it produced "topic" with no cluster name/uuid
suffix at all). This builds the ARN with explicit Fn::Split/Fn::Select/
Fn::Join instead, which resolves correctly at deploy time.
"""
import aws_cdk as cdk


def msk_resource_arn_wildcard(cluster_arn: str, resource_type: str) -> str:
    """Given an MSK cluster ARN token, return
    `arn:<partition>:kafka:<region>:<account>:<resource_type>/<cluster-name>/<cluster-uuid>/*`
    """
    colon_parts = cdk.Fn.split(":", cluster_arn)
    partition = cdk.Fn.select(1, colon_parts)
    region = cdk.Fn.select(3, colon_parts)
    account = cdk.Fn.select(4, colon_parts)
    resource_segment = cdk.Fn.select(5, colon_parts)  # "cluster/<name>/<uuid>"

    slash_parts = cdk.Fn.split("/", resource_segment)
    cluster_name = cdk.Fn.select(1, slash_parts)
    cluster_uuid = cdk.Fn.select(2, slash_parts)

    return cdk.Fn.join(
        "",
        [
            "arn:",
            partition,
            ":kafka:",
            region,
            ":",
            account,
            f":{resource_type}/",
            cluster_name,
            "/",
            cluster_uuid,
            "/*",
        ],
    )
