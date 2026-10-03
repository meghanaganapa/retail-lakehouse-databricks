#!/usr/bin/env bash
# Provision the Azure side of the lakehouse with the Azure CLI.
#   az login
#   ./infra/setup_azure.sh
#
# Creates: resource group, ADLS Gen2 storage (landing container), Databricks
# workspace (Premium, needed for Unity Catalog), an Access Connector (managed
# identity for storage access), Event Hubs for clickstream and a Key Vault.
#
# Costs money: delete everything afterwards with
#   az group delete -n "$RG" --yes
set -euo pipefail

PREFIX="${PREFIX:-retaillh}"
LOCATION="${LOCATION:-australiaeast}"
RG="${RG:-rg-${PREFIX}}"
SUFFIX="${SUFFIX:-$RANDOM}"
STORAGE="${PREFIX}st${SUFFIX}"          # 3-24 lowercase letters/digits
WORKSPACE="dbw-${PREFIX}"
CONNECTOR="ac-${PREFIX}"
EH_NAMESPACE="evh-${PREFIX}-${SUFFIX}"
KEYVAULT="kv-${PREFIX}-${SUFFIX}"

echo "Resource group $RG in $LOCATION"
az group create -n "$RG" -l "$LOCATION" -o none

echo "ADLS Gen2 storage $STORAGE"
az storage account create -n "$STORAGE" -g "$RG" -l "$LOCATION" \
  --sku Standard_LRS --kind StorageV2 --hns true --min-tls-version TLS1_2 \
  --allow-blob-public-access false -o none
az storage container create --account-name "$STORAGE" -n landing --auth-mode login -o none

echo "Databricks workspace $WORKSPACE (Premium)"
az extension add --name databricks --upgrade -y -o none
az databricks workspace create -n "$WORKSPACE" -g "$RG" -l "$LOCATION" --sku premium -o none

echo "Access Connector $CONNECTOR (managed identity for Unity Catalog)"
CONNECTOR_ID=$(az databricks access-connector create -n "$CONNECTOR" -g "$RG" -l "$LOCATION" \
  --identity-type SystemAssigned --query id -o tsv)
PRINCIPAL_ID=$(az databricks access-connector show -n "$CONNECTOR" -g "$RG" --query identity.principalId -o tsv)
STORAGE_ID=$(az storage account show -n "$STORAGE" -g "$RG" --query id -o tsv)
az role assignment create --assignee-object-id "$PRINCIPAL_ID" --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" --scope "$STORAGE_ID" -o none

echo "Event Hubs $EH_NAMESPACE/clickstream"
az eventhubs namespace create -n "$EH_NAMESPACE" -g "$RG" -l "$LOCATION" --sku Standard -o none
az eventhubs eventhub create -n clickstream --namespace-name "$EH_NAMESPACE" -g "$RG" --partition-count 2 -o none
az eventhubs eventhub authorization-rule create -n databricks --eventhub-name clickstream \
  --namespace-name "$EH_NAMESPACE" -g "$RG" --rights Listen Send -o none
EH_CONN=$(az eventhubs eventhub authorization-rule keys list -n databricks --eventhub-name clickstream \
  --namespace-name "$EH_NAMESPACE" -g "$RG" --query primaryConnectionString -o tsv)

echo "Key Vault $KEYVAULT (holds the Event Hubs connection string)"
az keyvault create -n "$KEYVAULT" -g "$RG" -l "$LOCATION" --enable-rbac-authorization true -o none
ME=$(az ad signed-in-user show --query id -o tsv)
KV_ID=$(az keyvault show -n "$KEYVAULT" --query id -o tsv)
az role assignment create --assignee "$ME" --role "Key Vault Secrets Officer" --scope "$KV_ID" -o none
sleep 30  # RBAC propagation
az keyvault secret set --vault-name "$KEYVAULT" -n eventhubs-connection-string --value "$EH_CONN" -o none

WS_URL=$(az databricks workspace show -n "$WORKSPACE" -g "$RG" --query workspaceUrl -o tsv)
cat <<EOF

Done. Next steps:
  1. Put https://$WS_URL in databricks.yml (workspace.host).
  2. Create a Key Vault-backed secret scope named "retail-kv" pointing at $KEYVAULT:
       https://$WS_URL#secrets/createScope
     (DNS name: https://$KEYVAULT.vault.azure.net/, resource ID: $KV_ID)
  3. In sql/unity_catalog_setup.sql use:
       access connector: $CONNECTOR_ID
       landing URL:      abfss://landing@$STORAGE.dfs.core.windows.net/
  4. For the clickstream producer: export EVENTHUB_CONNECTION_STRING from Key Vault and set
     eventhubs.namespace: $EH_NAMESPACE in resources/retail_pipeline.yml.
EOF
