/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package Goliath.Data.JDBC.JavaDB;

import Goliath.Data.DataType;
import Goliath.Interfaces.Data.IDataType;
import Goliath.Interfaces.Data.IRelation;

/**
 *
 * @author kenmchugh
 */
public class SQLGenerator<T extends Goliath.Data.DataObjects.SimpleDataObject>
        extends Goliath.DynamicCode.SQLGenerator<T>
{
    @Override
    protected String getLeftBoundry()
    {
        return "\"";
    }

    @Override
    protected String getRightBoundry()
    {
        return "\"";
    }

    @Override
    protected String onGetAutoIncrementText()
    {
        return "GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)";
    }

    @Override
    protected String getUTF8String()
    {
        return "";
    }

    @Override
    protected String dataTypeToString(IDataType toType)
    {
        if (toType == DataType.BOOLEAN())
        {
            return "SMALLINT";
        }
        return super.dataTypeToString(toType);
    }

    @Override
    protected String generateRelationString(IRelation toRelation)
    {
        return ", FOREIGN KEY (" + wrapInBounds(toRelation.getColumn().getName()) + ")" +
            " REFERENCES " + wrapInBounds(toRelation.getForeignTable().getName()) + " (" + wrapInBounds(toRelation.getForeignColumn().getName()) + ")";
    }

}
