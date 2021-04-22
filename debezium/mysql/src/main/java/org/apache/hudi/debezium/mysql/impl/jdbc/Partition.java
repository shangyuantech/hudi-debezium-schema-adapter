package org.apache.hudi.debezium.mysql.impl.jdbc;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hudi.debezium.mysql.data.PartitionMethod;

public class Partition {

    @JsonProperty("table_schema")
    private String tableSchema;

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("partition_name")
    private String partitionName;

    @JsonProperty("subpartition_name")
    private String subpartitionName;

    @JsonProperty("partition_ordinal_position")
    private String partitionOrdinalPosition;

    @JsonProperty("subpartition_ordinal_position")
    private String subpartitionOrdinalPosition;

    @JsonProperty("partition_method")
    private PartitionMethod partitionMethod;

    @JsonProperty("subpartition_method")
    private PartitionMethod subpartitionMethod;

    @JsonProperty("partition_expression")
    private String partitionExpression;

    @JsonProperty("subpartition_expression")
    private String subpartitionExpression;

    @JsonProperty("partition_description")
    private String partitionDescription;

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public String getSubpartitionName() {
        return subpartitionName;
    }

    public void setSubpartitionName(String subpartitionName) {
        this.subpartitionName = subpartitionName;
    }

    public String getPartitionOrdinalPosition() {
        return partitionOrdinalPosition;
    }

    public void setPartitionOrdinalPosition(String partitionOrdinalPosition) {
        this.partitionOrdinalPosition = partitionOrdinalPosition;
    }

    public String getSubpartitionOrdinalPosition() {
        return subpartitionOrdinalPosition;
    }

    public void setSubpartitionOrdinalPosition(String subpartitionOrdinalPosition) {
        this.subpartitionOrdinalPosition = subpartitionOrdinalPosition;
    }

    public PartitionMethod getPartitionMethod() {
        return partitionMethod;
    }

    public void setPartitionMethod(PartitionMethod partitionMethod) {
        this.partitionMethod = partitionMethod;
    }

    public PartitionMethod getSubpartitionMethod() {
        return subpartitionMethod;
    }

    public void setSubpartitionMethod(PartitionMethod subpartitionMethod) {
        this.subpartitionMethod = subpartitionMethod;
    }

    public String getPartitionExpression() {
        return partitionExpression;
    }

    public void setPartitionExpression(String partitionExpression) {
        this.partitionExpression = partitionExpression;
    }

    public String getSubpartitionExpression() {
        return subpartitionExpression;
    }

    public void setSubpartitionExpression(String subpartitionExpression) {
        this.subpartitionExpression = subpartitionExpression;
    }

    public String getPartitionDescription() {
        return partitionDescription;
    }

    public void setPartitionDescription(String partitionDescription) {
        this.partitionDescription = partitionDescription;
    }

    @Override
    public String toString() {
        return "Partition{" +
                "tableSchema='" + tableSchema + '\'' +
                ", tableName='" + tableName + '\'' +
                ", partitionName='" + partitionName + '\'' +
                ", subpartitionName='" + subpartitionName + '\'' +
                ", partitionOrdinalPosition='" + partitionOrdinalPosition + '\'' +
                ", subpartitionOrdinalPosition='" + subpartitionOrdinalPosition + '\'' +
                ", partitionMethod='" + partitionMethod + '\'' +
                ", subpartitionMethod='" + subpartitionMethod + '\'' +
                ", partitionExpression='" + partitionExpression + '\'' +
                ", subpartitionExpression='" + subpartitionExpression + '\'' +
                ", partitionDescription='" + partitionDescription + '\'' +
                '}';
    }
}
