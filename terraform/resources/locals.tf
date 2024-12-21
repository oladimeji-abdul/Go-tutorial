locals {
    instances = {
        # jenkins = {
        #   user_data = <<-EOT
        #     #!/bin/bash
        #     # Begin Docker Installation Script
        #     $(file("../scripts/install_docker.sh"))

        #     # Begin Jenkins Setup Script
        #     $(file("../scripts/setup_jenkins.sh"))
        #   EOT
        # }
    }
    security_groups = {
        instance = {
            
              }
            }
  tags = {
    Environment = "Dev"
  }
}