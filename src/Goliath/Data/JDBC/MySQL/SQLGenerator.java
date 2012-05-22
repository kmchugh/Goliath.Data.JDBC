/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package Goliath.Data.JDBC.MySQL;

import Goliath.Interfaces.Data.IRelation;

/**
 *
 * @author kenmchugh
 */
public class SQLGenerator<T extends Goliath.Data.DataObjects.SimpleDataObject> extends Goliath.DynamicCode.SQLGenerator<T>
{
    @Override
    protected String getLeftBoundry()
    {
        return "`";
    }

    @Override
    protected String getRightBoundry()
    {
        return "`";
    }

    @Override
    protected String onGetAutoIncrementText()
    {
        return "auto_increment";
    }

    @Override
    protected String generateRelationString(IRelation toRelation)
    {
        return ", CONSTRAINT " + wrapInBounds(toRelation.getName()) +
                " FOREIGN KEY (" + wrapInBounds(toRelation.getColumn().getName()) + ")" +
                " REFERENCES " + wrapInBounds(toRelation.getForeignTable().getName()) + "(" + wrapInBounds(toRelation.getForeignColumn().getName()) + ")" +
                " ON DELETE NO ACTION ON UPDATE NO ACTION";
    }
}
