# Fabric notebook source
# METADATA ********************
# META {
# META   "kernel_info": { 
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }
# CELL ********************
# Welcome to your new notebook
# Type here in the cell editor to add code!

import os
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

# Dynamically get current working directory for repository_directory (works in automation pipelines)
repository_directory = os.getcwd()

# Initialize the FabricWorkspace object with the required parameters
target_workspace = FabricWorkspace(
    workspace_id = "d1ecda06-105b-4dcb-9af8-d14c17383377",
    repository_directory = repository_directory,
    item_type_in_scope = ["Notebook","DataPipeline","Environment"],
)

# Publish all items defined in item_type_in_scope
publish_all_items(target_workspace)

# Unpublish all items defined in item_type_in_scope not found in repository
unpublish_all_orphan_items(target_workspace)

# METADATA ********************
# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }