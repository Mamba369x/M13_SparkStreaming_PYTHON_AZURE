provider "azurerm" {
  features {}

  client_id       = var.CLIENT_ID
  client_secret   = var.CLIENT_SECRET
  tenant_id       = var.TENANT_ID
  subscription_id = var.SUBSCRIPTION_ID
}

provider "databricks" {
  host                        = data.azurerm_databricks_workspace.bdcc.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.bdcc.id

  azure_client_id     = var.CLIENT_ID
  azure_client_secret = var.CLIENT_SECRET
  azure_tenant_id     = var.TENANT_ID
}

resource "azurerm_resource_group" "bdcc" {
  name     = "rg${var.ENV}${var.LOCATION}"
  location = var.LOCATION

  tags = {
    region = var.BDCC_REGION
    env    = var.ENV
  }
}

resource "azurerm_storage_account" "bdcc" {
  name                     = "sa${var.ENV}${var.LOCATION}"
  resource_group_name      = azurerm_resource_group.bdcc.name
  location                 = azurerm_resource_group.bdcc.location
  account_tier             = "Standard"
  account_replication_type = var.STORAGE_ACCOUNT_REPLICATION_TYPE
  is_hns_enabled           = true
  allow_blob_public_access = true

  network_rules {
    default_action = "Allow"
    ip_rules       = values(var.IP_RULES)
  }

  tags = {
    region = var.BDCC_REGION
    env    = var.ENV
  }
}

resource "azurerm_storage_container" "bdcc" {
  name                  = "data${var.ENV}${var.LOCATION}"
  storage_account_name  = azurerm_storage_account.bdcc.name
  container_access_type = "blob"

  lifecycle {
    prevent_destroy = false
  }
}

resource "azurerm_storage_blob" "bdcc" {
  for_each               = fileset(path.module, "../m13sparkstreaming/**")
  name                   = replace(each.key, "../m13sparkstreaming/", "")
  storage_account_name   = azurerm_storage_account.bdcc.name
  storage_container_name = azurerm_storage_container.bdcc.name
  type                   = "Block"
  source                 = each.key
}

resource "azurerm_databricks_workspace" "bdcc" {
  depends_on = [
    azurerm_resource_group.bdcc
  ]

  name                = "dbw${var.ENV}${var.LOCATION}"
  resource_group_name = azurerm_resource_group.bdcc.name
  location            = azurerm_resource_group.bdcc.location
  sku                 = "standard"

  tags = {
    region = var.BDCC_REGION
    env    = var.ENV
  }
}

data "azurerm_databricks_workspace" "bdcc" {
  name                = azurerm_databricks_workspace.bdcc.name
  resource_group_name = azurerm_resource_group.bdcc.name
}


resource "databricks_cluster" "bdcc" {
  cluster_name            = "cluster${var.ENV}${var.LOCATION}"
  spark_version           = "12.2.x-cpu-ml-scala2.12"
  node_type_id            = "Standard_D8s_v3"

  num_workers = 8

  spark_conf = {
    "spark.databricks.cluster.profile" : "singleNode"
    "spark.master" : "local[*]"
  }

  spark_env_vars = {
    STORAGE_ACCOUNT_NAME   = azurerm_storage_account.bdcc.name
    STORAGE_CONTAINER_NAME = azurerm_storage_container.bdcc.name
    TF_VAR_CLIENT_ID       = var.CLIENT_ID
    TF_VAR_CLIENT_SECRET   = var.CLIENT_SECRET
    TF_VAR_TENANT_ID       = var.TENANT_ID
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
}


resource "databricks_notebook" "bdcc" {
  source     = "${path.module}/../notebooks/stream.ipynb"
  path       = "/notebooks/stream"
  depends_on = [databricks_cluster.bdcc]
}

output "storage_account_name" {
  value     = azurerm_storage_account.bdcc.name
  sensitive = true
}

output "storage_container_name" {
  value     = azurerm_storage_container.bdcc.name
  sensitive = true
}

output "resource_group_name" {
  value     = azurerm_resource_group.bdcc.name
  sensitive = true
}

output "databricks_workspace_url" {
  value     = data.azurerm_databricks_workspace.bdcc.workspace_url
  sensitive = true
}

output "databricks_workspace_id" {
  value     = data.azurerm_databricks_workspace.bdcc.id
  sensitive = true
}
