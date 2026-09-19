"""Minimal active-alias identity proof, never returning environment configuration."""


def active_alias(client,function,receipt):
    """Return a proof only for a fully promoted live alias on the intended bytes."""
    alias=client.get_alias(FunctionName=function,Name='live')
    version=alias.get('FunctionVersion')
    if not isinstance(version,str) or not version.isdigit():return None
    if (alias.get('RoutingConfig') or {}).get('AdditionalVersionWeights'):return None
    config=client.get_function_configuration(FunctionName=function,Qualifier='live')
    sha=receipt.get('code_sha256')
    if not sha or config.get('Version')!=version or config.get('CodeSha256')!=sha:return None
    return {'function':function,'alias':'live','version':version,'code_sha256':sha,'commit':receipt['commit']}
