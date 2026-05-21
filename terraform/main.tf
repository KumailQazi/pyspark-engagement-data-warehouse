# Azure Infrastructure provisioning for Tapmad Data Platform
# Requires Azure RM Provider

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# 1. Resource Group
resource "azurerm_resource_group" "tapmad_data_rg" {
  name     = "rg-tapmad-data-prod"
  location = "East US"
  tags = {
    Environment = "Production"
    Team        = "Data Engineering"
  }
}

# 2. ADLS Gen2 (The Lakehouse Foundation)
resource "azurerm_storage_account" "tapmad_adls" {
  name                     = "sttapmaddataprod"
  resource_group_name      = azurerm_resource_group.tapmad_data_rg.name
  location                 = azurerm_resource_group.tapmad_data_rg.location
  account_tier             = "Standard"
  account_replication_type = "ZRS" # Zone-redundant for high availability
  is_hns_enabled           = true  # Required for ADLS Gen2 hierarchical namespace

  tags = {
    Purpose = "Bronze, Silver, Gold Delta Lakes"
  }
}

# 3. Azure Event Hubs (Kafka compatible streaming ingestion)
resource "azurerm_eventhub_namespace" "tapmad_eh_ns" {
  name                = "evhns-tapmad-streaming-prod"
  location            = azurerm_resource_group.tapmad_data_rg.location
  resource_group_name = azurerm_resource_group.tapmad_data_rg.name
  sku                 = "Standard"
  capacity            = 10 # Scaled for 100M+ daily events
}

resource "azurerm_eventhub" "playback_events" {
  name                = "playback-events"
  namespace_name      = azurerm_eventhub_namespace.tapmad_eh_ns.name
  resource_group_name = azurerm_resource_group.tapmad_data_rg.name
  partition_count     = 32 # High partition count for parallel Spark streaming reads
  message_retention   = 7  # 7 days retention for replayability
}

# 4. Azure Databricks Workspace (Compute)
resource "azurerm_databricks_workspace" "tapmad_dbx" {
  name                = "dbw-tapmad-compute-prod"
  resource_group_name = azurerm_resource_group.tapmad_data_rg.name
  location            = azurerm_resource_group.tapmad_data_rg.location
  sku                 = "premium"
}
