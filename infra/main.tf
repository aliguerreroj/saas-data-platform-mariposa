# Módulo Terraform ilustrativo -- ver docs/infra.md para el contexto completo.
# NO está pensado para aplicarse tal cual: modela cómo se vería aprovisionar
# el storage por ambiente en un cloud real (AWS), manteniendo el mismo mapeo
# de paths que ya usa el pipeline localmente (config/base.yaml -> paths.*).

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "environment" {
  description = "dev | qa | main -- debe coincidir con config/env/<environment>.yaml"
  type        = string
  validation {
    condition     = contains(["dev", "qa", "main"], var.environment)
    error_message = "environment debe ser uno de: dev, qa, main."
  }
}

variable "known_tenants" {
  description = "Codigos de tenant conocidos -- debe reflejar config/tenants/*.yaml"
  type        = list(string)
  default     = ["gt", "sv", "hn", "jm", "pe", "ec"]
}

# Un bucket por ambiente, con las mismas capas que usa el pipeline localmente
# (bronze/silver/gold/quarantine/shared) como prefijos dentro del mismo
# bucket -- el mapeo config -> infra queda 1:1, sin traducir paths a mano.
resource "aws_s3_bucket" "datalake" {
  bucket = "grupo-mariposa-datalake-${var.environment}"
}

resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Aislamiento por tenant a nivel de IAM -- la version "de verdad" del
# aislamiento que hoy el pipeline logra solo por convención de paths (ver
# docs/observations.md y la sección "Qué dejé fuera" del README). Un rol por
# tenant que solo puede leer/escribir bajo su propio prefijo dentro de cada
# capa, en vez de depender de que ningún proceso escriba fuera de su path.
resource "aws_iam_policy" "tenant_scoped_access" {
  for_each = toset(var.known_tenants)

  name = "mariposa-${var.environment}-${each.key}-datalake-access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.datalake.arn,
        "${aws_s3_bucket.datalake.arn}/data/*/${each.key}/*",
      ]
      Condition = {
        StringLike = { "s3:prefix" = ["data/*/${each.key}/*"] }
      }
    }]
  })
}

output "bucket_name" {
  value = aws_s3_bucket.datalake.bucket
}