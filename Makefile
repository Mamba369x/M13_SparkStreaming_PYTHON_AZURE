# Ensure TF_VAR_SUBSCRIPTION_ID is set before running the target
ifndef TF_VAR_SUBSCRIPTION_ID
$(error TF_VAR_SUBSCRIPTION_ID is undefined. Please set TF_VAR_SUBSCRIPTION_ID to your Azure subscription ID.)
endif

SET_ENV = export
CLEAN = rm -rf terraform/.terraform terraform/.terraform.lock.hcl terraform/terraform.tfstate terraform/terraform.tfstate.backup terraform/env.sh
AZURE_LOGIN = az login
AZURE_SP_CREATE = az ad sp create-for-rbac --name "homework" --role "Owner" --scopes "/subscriptions/$$TF_VAR_SUBSCRIPTION_ID"

start:
	$(AZURE_LOGIN) && \
	SP_JSON=$$($(AZURE_SP_CREATE)) && \
	$(SET_ENV) TF_VAR_CLIENT_ID=$$(echo $${SP_JSON} | jq -r .appId) && \
	$(SET_ENV) TF_VAR_CLIENT_SECRET=$$(echo $${SP_JSON} | jq -r .password) && \
	$(SET_ENV) TF_VAR_TENANT_ID=$$(echo $${SP_JSON} | jq -r .tenant) && \
	sleep 65 && \
	az role assignment create --assignee $${TF_VAR_CLIENT_ID} --role "Storage Blob Data Contributor" --scope "/subscriptions/$${TF_VAR_SUBSCRIPTION_ID}" && \
	cd terraform && \
	terraform init && \
	terraform apply -auto-approve && \
	$(SET_ENV) RESOURCE_GROUP_NAME=$$(terraform output -json | jq -r .resource_group_name.value) && \
	$(SET_ENV) STORAGE_ACCOUNT_NAME=$$(terraform output -json | jq -r .storage_account_name.value) && \
	$(SET_ENV) STORAGE_CONTAINER_NAME=$$(terraform output -json | jq -r .storage_container_name.value) && \
	$(SET_ENV) DATABRICKS_WORKSPACE_URL=$$(terraform output -json | jq -r .databricks_workspace_url.value) && \
	$(SET_ENV) DATABRICKS_WORKSPACE_ID=$$(terraform output -json | jq -r .databricks_workspace_id.value) && \
	echo "$(SET_ENV) TF_VAR_CLIENT_ID=$$TF_VAR_CLIENT_ID" >> env.sh && \
	echo "$(SET_ENV) TF_VAR_CLIENT_SECRET=$$TF_VAR_CLIENT_SECRET" >> env.sh && \
	echo "$(SET_ENV) TF_VAR_TENANT_ID=$$TF_VAR_TENANT_ID" >> env.sh && \
	echo "$(SET_ENV) RESOURCE_GROUP_NAME=$$RESOURCE_GROUP_NAME" >> env.sh && \
	echo "$(SET_ENV) STORAGE_ACCOUNT_NAME=$$STORAGE_ACCOUNT_NAME" >> env.sh  && \
	echo "$(SET_ENV) STORAGE_CONTAINER_NAME=$$STORAGE_CONTAINER_NAME" >> env.sh && \
	echo "$(SET_ENV) DATABRICKS_WORKSPACE_URL=$$DATABRICKS_WORKSPACE_URL" >> env.sh && \
	echo "$(SET_ENV) DATABRICKS_WORKSPACE_ID=$$DATABRICKS_WORKSPACE_ID" >> env.sh

upload:
	cd terraform && \
	source env.sh && \
	cd .. && \
	spark-submit \
		--master local[*] \
		--conf spark.executor.instances=4 \
		--conf spark.jars.packages=org.apache.hadoop:hadoop-azure:3.3.1,org.apache.hadoop:hadoop-azure-datalake:3.3.1,com.microsoft.azure:azure-storage:8.6.6 \
			notebooks/upload.py \
		--storageaccount $$STORAGE_ACCOUNT_NAME \
		--container $$STORAGE_CONTAINER_NAME \
		--tfvarclientid $$TF_VAR_CLIENT_ID \
		--tfvarclientsecret $$TF_VAR_CLIENT_SECRET \
		--tfvartenantid $$TF_VAR_TENANT_ID \
		--opencageapikey $$OPENCAGEAPIKEY 2>&1 | tee "upload.log"

run:
	cd terraform && \
	source env.sh && \
	cd .. && \
	spark-submit \
		--master local[*] \
		--conf spark.executor.instances=4 \
		--conf spark.jars.packages=org.apache.hadoop:hadoop-azure:3.3.1,org.apache.hadoop:hadoop-azure-datalake:3.3.1,com.microsoft.azure:azure-storage:8.6.6 \
			notebooks/streaming.py \
		--storageaccount $$STORAGE_ACCOUNT_NAME \
		--container $$STORAGE_CONTAINER_NAME \
		--tfvarclientid $$TF_VAR_CLIENT_ID \
		--tfvarclientsecret $$TF_VAR_CLIENT_SECRET \
		--tfvartenantid $$TF_VAR_TENANT_ID \
		--opencageapikey $$OPENCAGEAPIKEY 2>&1 | tee "stream.log"

destroy:
	cd terraform && \
	source env.sh && \
	terraform destroy -auto-approve && \
	cd .. && \
	$(CLEAN)

clean:
	$(CLEAN)