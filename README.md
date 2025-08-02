# Automation For Extracting Data From Confluence API
This automation downloads the following resources from the confluence API:
- Spaces
- Pages
- Attachments
- Blogposts
- Comments
- Tasks

The data is stored in Google BigQuery and the attachments are stored in Google Cloud Storage.

## Setup
Create a `.env` file in the project root directory and set the following variables:
```bash

    BASE_URL="https://{your-atlassian-domain}/wiki/api/v2/"
    EMAIL="<email with access to confluence>"
    API_TOKEN="<API token>"
    PAGES_TABLE="pages"
    COMMENTS_TABLE="comments"
    ATTACHMENTS_TABLE="attachment"
    SPACES_TABLE="spaces"
    TASKS_TABLE="tasks"
    BLOGPOSTS_TABLE="blogposts"
    PROJECT_NAME="<GCP Project name>"
    DATASET="<BigQuery dataset name>"
    GCP_STORAGE_BUCKET="<Cloud Storage bucket name>"
    GOOGLE_APPLICATION_CREDENTIALS="<path to your GCP service account JSON>"
    GCP_LOGGING_SERVICE_NAME="<Cloud Logging logger name>"
    TMP_DOWNLOADS_FOLDER="<downloads folder>"

```

- Obtaining the Confluence API token -> [docs](https://developer.atlassian.com/cloud/confluence/basic-auth-for-rest-apis/)
- Obtaining GCP service account credentials -> [docs](https://developers.google.com/workspace/guides/create-credentials#service-account)

Grant the following permissions to your Service Account
```bash

    - Storage Object Creator
    - Storage Object User
    - Storage Object Viewer
    - BigQuery Admin
    - BigQuery Data Editor
    - BigQuery Data Owner
    - BigQuery Data Viewer
    - BigQuery Job User
    - Logs Writer

```

## Running The Automation
Execute the following command in your project folder terminal to run the application:
```bash

docker compose up

```

## License
Refer tp the [LICENSE](LICENSE) for terms of use.

## Contributions
Contributions are very much welcome.<br>
If you encounter problems feel free to open an issue.