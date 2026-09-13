"""Entrypoint for the JustHodl bounded student factory.

Keep the conventional Lambda handler so deployment receipts and the repository
stub guard can recognize the runnable entrypoint. All work is implemented in
student_lambda and the versioned factory modules bundled by the existing
release pipeline. This Lambda accepts only the six documented task names or
an empty scheduled tick. It has no Function URL, no deployment credentials,
no ability to create roles or schedules, and no endpoint creation permission.
"""
from student_lambda import lambda_handler as student_handler


def lambda_handler(event, context):
    return student_handler(event, context)
