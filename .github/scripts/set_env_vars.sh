# Get Databricks host url
DATABRICKS_HOST="$DATABRICKS_HOST"
echo "::add-mask::$DATABRICKS_HOST"
echo "DATABRICKS_HOST: $DATABRICKS_HOST"

# Get Databricks personal access token (PAT)
DATABRICKS_TOKEN="$DATABRICKS_TOKEN"
echo "::add-mask::$DATABRICKS_TOKEN"
echo "DATABRICKS_TOKEN: $DATABRICKS_TOKEN"

# Get target environment for DABs
TARGET="$TARGET"
echo "TARGET: $TARGET"

# Get branch name
export BRANCH_NAME="$GITHUB_REF_NAME"
echo "BRANCH_NAME: $BRANCH_NAME"

# Get email
USER_EMAIL="$USER_EMAIL"
echo "::add-mask::$USER_EMAIL"
echo "USER_EMAIL: $USER_EMAIL"

# Get ODCS contract source catalog
ODCS_CONTRACT_SOURCE_CATALOG="$ODCS_CONTRACT_SOURCE_CATALOG"
echo "ODCS_CONTRACT_SOURCE_CATALOG: $ODCS_CONTRACT_SOURCE_CATALOG"

# Get ODCS contract source schema
ODCS_CONTRACT_SOURCE_SCHEMA="$ODCS_CONTRACT_SOURCE_SCHEMA"
echo "ODCS_CONTRACT_SOURCE_SCHEMA: $ODCS_CONTRACT_SOURCE_SCHEMA"

# Convert email to username
USER_NAME=$(echo "$USER_EMAIL" | cut -d '@' -f 1 | tr '.' '_')
echo "USER_NAME: $USER_NAME"

# Optionally make them available to later steps
echo "DATABRICKS_HOST=$DATABRICKS_HOST" >> $GITHUB_ENV
echo "DATABRICKS_TOKEN=$DATABRICKS_TOKEN" >> $GITHUB_ENV
echo "TARGET=$TARGET" >> $GITHUB_ENV
echo "BRANCH_NAME=$BRANCH_NAME" >> $GITHUB_ENV
echo "ODCS_CONTRACT_SOURCE_CATALOG=$ODCS_CONTRACT_SOURCE_CATALOG" >> $GITHUB_ENV
echo "ODCS_CONTRACT_SOURCE_SCHEMA=$ODCS_CONTRACT_SOURCE_SCHEMA" >> $GITHUB_ENV
echo "USER_EMAIL=$USER_EMAIL" >> $GITHUB_ENV
echo "USER_NAME=$USER_NAME" >> $GITHUB_ENV
echo "GIT_REPO_URL=https://github.com/${GITHUB_REPOSITORY}.git" >> $GITHUB_ENV

if [[ "$GITHUB_ACTIONS" == "true" ]]; then
echo "Running inside GitHub CI/CD"
echo "GIT_SOURCE=gitHub" >> $GITHUB_ENV
elif [[ "$GITLAB_CI" == "true" ]]; then
echo "Running inside GitLab CI/CD"
echo "GIT_SOURCE=gitLab" >> $GITHUB_ENV
else
echo "Not running inside GitHub or GitLab"
fi