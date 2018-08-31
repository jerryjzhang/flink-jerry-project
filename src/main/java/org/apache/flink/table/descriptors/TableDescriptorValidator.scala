package org.apache.flink.table.descriptors

import java.util

import org.apache.flink.table.descriptors.StreamTableDescriptorValidator.{UPDATE_MODE, UPDATE_MODE_VALUE_APPEND, UPDATE_MODE_VALUE_RETRACT, UPDATE_MODE_VALUE_UPSERT}

class TableDescriptorValidator extends DescriptorValidator{
  override def validate(properties: DescriptorProperties): Unit = {
    properties.validateEnumValues(
      UPDATE_MODE,
      isOptional = false,
      util.Arrays.asList(
        UPDATE_MODE_VALUE_APPEND,
        UPDATE_MODE_VALUE_RETRACT,
        UPDATE_MODE_VALUE_UPSERT)
    )
  }
}

object TableDescriptorValidator {

  val UPDATE_MODE = "update-mode"
  val UPDATE_MODE_VALUE_APPEND = "append"
  val UPDATE_MODE_VALUE_RETRACT = "retract"
  val UPDATE_MODE_VALUE_UPSERT = "upsert"
}