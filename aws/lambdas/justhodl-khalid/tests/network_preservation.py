"""Only the new output adapter is removed for earlier decision-parity gates."""
def without_network(text):
    block = '    from research_network_consumer import attach\n    attach(payload, S3, BUCKET, "khalid")\n'
    assert text.count(block) == 1
    return text.replace(block, '')
