import asyncio

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import awsgi
from app import app  # Import your Flask app instance


def handler(event, context):
    """
    AWS Lambda handler for the Flask app
    """
    return awsgi.response(app, event, context)
