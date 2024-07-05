variable "ENV" {
  type        = string
  description = "The prefix which should be used for all resources in this environment. Make it unique, like ksultanau."
  default     = "sql"
}

variable "LOCATION" {
  type        = string
  description = "The Azure Region in which all resources should be created."
  default     = "westeurope"
}

variable "BDCC_REGION" {
  type        = string
  description = "The BDCC Region for billing."
  default     = "global"
}

variable "STORAGE_ACCOUNT_REPLICATION_TYPE" {
  type        = string
  description = "Storage Account replication type."
  default     = "LRS"
}

variable "IP_RULES" {
  type        = map(string)
  description = "Map of IP addresses permitted to access"
  default = {
    "epam-vpn-ru-0" = "185.44.13.36"
    "epam-vpn-eu-0" = "195.56.119.209"
    "epam-vpn-by-0" = "213.184.231.20"
    "epam-vpn-by-1" = "86.57.255.94"
  }
}

variable "CLIENT_ID" {
  type        = string
  description = "The Client ID of the Service Principal used for authentication"
}

variable "CLIENT_SECRET" {
  type        = string
  description = "The Client Secret of the Service Principal used for authentication"
}

variable "SUBSCRIPTION_ID" {
  type        = string
  description = "The Subscription ID where the resources will be created"
}

variable "TENANT_ID" {
  type        = string
  description = "The Tenant ID of the Azure Active Directory"
}
