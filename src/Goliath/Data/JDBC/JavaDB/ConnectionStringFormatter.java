/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package Goliath.Data.JDBC.JavaDB;

import Goliath.Collections.List;
import Goliath.Constants.StringFormatType;
import Goliath.Interfaces.Data.IConnectionString;
import Goliath.Text.StringFormatter;

/**
 *
 * @author kenmchugh
 */
public class ConnectionStringFormatter extends Goliath.Text.StringFormatter<Goliath.Interfaces.Data.IConnectionString>
{
    /** Creates a new instance of JDBCMySQLStringFormatter */
    public ConnectionStringFormatter()
    {
    }

    @Override
    public String toString(Goliath.Interfaces.Data.IConnectionString toObject)
    {
        /*
        StringBuilder loBuilder = new StringBuilder("jdbc:derby:" + toObject.getParameter("dataDirectory") + toObject.getParameter("database") + ";databaseName="  + toObject.getParameter("database") + ";");

        for (Goliath.Interfaces.IProperty loParam : toObject.getParameters().values())
        {
            if (!loParam.getName().equalsIgnoreCase("database") &&
                 loParam.getValue() != null)
            {
                loBuilder.append(loParam.getName() + "=" + loParam.getValue().toString() + ";");
            }
        }
        return loBuilder.toString();
         * */

        StringBuilder loBuilder = new StringBuilder("jdbc:derby:" + toObject.getParameter("dataDirectory") + toObject.getParameter("database") + ";");
        if (toObject.getParameter("create") != null)
        {
            loBuilder.append("create=" + toObject.getParameter("create").toString() + ";");
        }

        for (String lcParameterName : toObject.getParameters().getPropertyKeys())
        {
            if (!lcParameterName.equalsIgnoreCase("database") &&
                !lcParameterName.equalsIgnoreCase("create") &&
                !lcParameterName.equalsIgnoreCase("dataDirectory") &&
                 toObject.getParameters().getProperty(lcParameterName) != null)
            {
                loBuilder.append(lcParameterName + "=" + toObject.getParameters().getProperty(lcParameterName) .toString() + ";");
            }
        }

        return loBuilder.toString();

    }

    @Override
    public void appendPrimitiveString(StringBuilder toBuilder, IConnectionString toObject, StringFormatType toType)
    {
        
    }

    @Override
    protected void formatComplexProperty(StringBuilder toBuilder, String tcPropertyName, Object toValue, StringFormatter toFormatter, StringFormatType toType)
    {
        
    }

    @Override
    protected void formatForPropertyCount(StringBuilder toBuilder, int tnIndex, int tnCount, StringFormatType toFormatType)
    {
        
    }

    @Override
    public String formatNullObject()
    {
        return "";
    }

    @Override
    protected String getEndTag(IConnectionString toObject)
    {
        return "";
    }

    @Override
    protected String getStartTag(IConnectionString toObject)
    {
        return "";
    }

    @Override
    public List<StringFormatType> supportedFormats()
    {
        return new List<StringFormatType>(new StringFormatType[]{StringFormatType.DEFAULT()});
    }

}
