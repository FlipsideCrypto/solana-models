import requests
import os
import sys

def create_message():
    """Creates a simple failure notification message with repo, workflow name, and URL"""

    # Get GitHub environment variables
    repository = os.environ.get('GITHUB_REPOSITORY', 'Unknown repository')
    repo_name = repository.split('/')[-1] if '/' in repository else repository
    workflow_name = os.environ.get('GITHUB_WORKFLOW', 'Unknown workflow')
    run_id = os.environ.get('GITHUB_RUN_ID', '')
    server_url = os.environ.get('GITHUB_SERVER_URL', 'https://github.com')

    # Build the workflow URL
    workflow_url = f"{server_url}/{repository}/actions/runs/{run_id}"

    message_body = {
        "text": f"Failure in {repo_name}",
        "attachments": [
            {
                "color": "#f44336",  # Red color for failures
                "fields": [
                    {
                        "title": "Repository",
                        "value": repository,
                        "short": True
                    },
                    {
                        "title": "Workflow",
                        "value": workflow_name,
                        "short": True
                    }
                ],
                "actions": [
                    {
                        "type": "button",
                        "text": "View Workflow Run",
                        "style": "primary",
                        "url": workflow_url
                    }
                ],
                "footer": "GitHub Actions"
            }
        ]
    }

    return message_body

def send_alert(webhook_url):
    """Sends a failure notification to Slack"""

    message = create_message()

    try:
        response = requests.post(webhook_url, json=message)

        if response.status_code == 200:
            print("Successfully sent Slack notification")
        else:
            print(f"Failed to send Slack notification: {response.status_code} {response.text}")
            sys.exit(1)
    except Exception as e:
        print(f"Error sending Slack notification: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")

    if not webhook_url:
        print("ERROR: SLACK_WEBHOOK_URL environment variable is required")
        sys.exit(1)

    send_alert(webhook_url)