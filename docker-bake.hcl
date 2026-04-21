variable "VERSION" {
  default = "2.0.2"
}

variable "REGISTRY" {
  default = "docker.io"
}

variable "IMAGE" {
  default = "zeabur/oci-ro-registry"
}

function "tags" {
  params = [version]
  result = [
    "${REGISTRY}/${IMAGE}:${version}",
    "${REGISTRY}/${IMAGE}:${join(".", slice(split(".", version), 0, 2))}",
    "${REGISTRY}/${IMAGE}:${element(split(".", version), 0)}",
    "${REGISTRY}/${IMAGE}:latest",
  ]
}

group "default" {
  targets = ["registry"]
}

target "registry" {
  context    = "."
  dockerfile = "Dockerfile"
  platforms  = ["linux/amd64", "linux/arm64"]
  tags       = tags(VERSION)
  labels = {
    "org.opencontainers.image.source" = "https://github.com/zeabur/oci-ro-registry-go"
    "org.opencontainers.image.author" = "contact@zeabur.com"
  }
}
