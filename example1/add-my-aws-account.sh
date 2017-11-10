#!/bin/bash

# Run this to set your AWS account in the docker.sbt.
# Do not commit that.  Just set it before launching a build, so it names it and publishes.

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR

ACCOUNT_ID=$(aws iam get-user | grep "Arn\":" | sed s/^.*arn:aws:iam::// | sed "s/:.*//")
REGION=$(aws configure get region)
FILE=docker.sbt

if [ "$ACCOUNT_ID" == "" ]; then
  echo "Error getting aws account!  Set ~/.aws/credentials for this profile? $AWS_DEFAULT_PROFILE"
  exit 1
fi

if [ "$REGION" == "" ]; then
  echo "Error getting aws region!  Set ~/.aws/config for this profile? $AWS_DEFAULT_PROFILE"
  exit 1
fi

echo "Updating $FILE to AWS account $ACCOUNT_ID"
echo "Updating $FILE to AWS region $REGION"

sed -i.bak1 "s/val awsAccountId = \".*\"/val awsAccountId = \"$ACCOUNT_ID\"/" $FILE 
grep "$ACCOUNT_ID" $FILE || (echo "Error setting account ID" && exit 1)
rm ${FILE}.bak1

sed -i.bak2 "s/val awsRegion = \".*\"/val awsRegion = \"$REGION\"/" $FILE 
grep "$REGION" $FILE || (echo "Error setting region" && exit 1)
rm ${FILE}.bak2 

