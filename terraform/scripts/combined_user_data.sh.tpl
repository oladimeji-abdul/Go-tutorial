#!/bin/bash
# Docker Installation Script
%{file("scripts/setup_docker.sh")}

# Jenkins Setup Script
%{file("scripts/setup_jenkins.sh")}
