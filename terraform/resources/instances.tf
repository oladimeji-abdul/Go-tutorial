data "aws_ssm_parameter" "ubuntu_20_04" {
  name = "/aws/service/canonical/ubuntu/server/20.04/stable/current/amd64/hvm/ebs-gp2/ami-id"
}

resource "aws_instance" "instances" {
  for_each = local.instances
  ami           = data.aws_ssm_parameter.ubuntu_20_04.value
  instance_type = "t2.micro"

  root_block_device {
    volume_size = 30 
    volume_type = "gp3"
    delete_on_termination = true
  }

  ebs_block_device {
  device_name           = "/dev/sdf"
  volume_size           = 100
  volume_type           = "gp3"
  delete_on_termination = true
 }

 user_data = lookup(each.value, "user_data", null)
 vpc_security_group_ids = [aws_security_group.instance.id]



  # network_interface {
  #   network_interface_id = aws_network_interface.foo.id
  #   device_index         = 0
  # }

  # credit_specification {
  #   cpu_credits = "unlimited"
  # }
  tags = merge(local.tags, {
    Name = each.key
  })
}