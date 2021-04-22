package org.apache.hudi.debezium.zookeeper.task;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang.StringUtils;

public class AlterField {

    private String oldName;

    private String newName;

    private String oldType;

    private String newType;

    public String getOldName() {
        return oldName;
    }

    public void setOldName(String oldName) {
        this.oldName = oldName;
    }

    public String getNewName() {
        return newName;
    }

    public void setNewName(String newName) {
        this.newName = newName;
    }

    public String getOldType() {
        return oldType;
    }

    public void setOldType(String oldType) {
        this.oldType = oldType;
    }

    public String getNewType() {
        return newType;
    }

    public void setNewType(String newType) {
        this.newType = newType;
    }

    @JsonIgnore
    public String getFieldsDesc() {
        StringBuilder desc = new StringBuilder();
        if (StringUtils.isNotBlank(oldName)) {
            desc.append(oldName);
            if (StringUtils.isNotBlank(oldType)) {
                desc.append("(").append(oldType).append(")");
            }
            desc.append("|");
        }
        if (StringUtils.isNotBlank(newName)) {
            desc.append(newName);
            if (StringUtils.isNotBlank(newType)) {
                desc.append("(").append(newType).append(")");
            }
        }

        return desc.toString();
    }

    @Override
    public String toString() {
        return "AlterField{" +
                "oldName='" + oldName + '\'' +
                ", newName='" + newName + '\'' +
                ", oldType='" + oldType + '\'' +
                ", newType='" + newType + '\'' +
                '}';
    }
}
