**This page is under construction. Do not use these instructions.**
<a href='Hidden comment: 
Summary should be something like:
Improving indexing performance with the 2.8.4 release
'></a>

# Introduction #

This document describes steps to improve indexing performance in certain circumstances when using connector version 2.8.4

# Prerequisites #
  * Database connector version 2.8.4 is required.
  * The SQL query must be ordered by a single, integer-valued column that provides a total ordering (e.g., a sequence or identity primary key).
  * **Note:** The SQL query for the database connector must always have an `ORDER BY` clause that imposes a total ordering, even when not using this optimization. Using an unordered query will result in erratic behavior, deleting and reindexing database rows.

# Configuring the connector instance #

You will need to edit the connectorInstance.xml file. The instructions differ for GSA 6.14 or later.

## Using GSA 6.14 or later ##
> <ol>
<blockquote><li> In the GSA admin console, create or edit a connector instance.<br>
<li> For a new connector, fill out the configuration form as usual.<br>
<li> In the <b>SQL Query</b> field, include a clause of the form <i>key</i> <code>&gt; #value#</code>, for example,<br>
<pre><code>     select * from employee where emp_id &gt; #value# order by emp_id<br>
</code></pre>
<li> Under <b>Advanced properties</b>, click <b>Show/Hide</b>.<br>
<li> Uncomment the following line:<br>
<pre><code>    &lt;!--<br>
    &lt;property name="parameterizedQueryFlag" value="true"&gt;&lt;/property&gt;<br>
    --&gt;<br>
</code></pre>
so that it reads:<br>
<pre><code>    &lt;property name="parameterizedQueryFlag" value="true"&gt;&lt;/property&gt;<br>
</code></pre>
<li> If necessary (this is unlikely), uncomment the <code>minValue</code> property and supply a value other than the default value of -1.<br>
<li> Click the <b>Save Configuration</b> button.</blockquote>

<h2>Using GSA 6.12 or earlier</h2>
<blockquote><ol>
<li> In the GSA admin console, create or edit a connector instance.<br>
<li> For a new connector, fill out the configuration form as usual.<br>
<li> In the <b>SQL Query</b> field, include a clause of the form <i>key</i> <code>&gt; #value#</code>, for example,<br>
<pre><code>     select * from employee where emp_id &gt; #value# order by emp_id<br>
</code></pre>
<li> Under <b>Disable Traversal</b>, select the checkbox.<br>
<li> Click the <b>Save Configuration</b> button.<br>
<li> In a file browser or command window, go to your connector installation directory, and then go to the Tomcat/webapps/connector-manager/WEB-INF/connectors/DBConnector directory, and then go to the directory with your connector instance name.<br>
<li> Open connectorInstance.xml in a text editor.<br>
<li> Uncomment the following line:<br>
<pre><code>    &lt;!--<br>
    &lt;property name="parameterizedQueryFlag" value="true"&gt;&lt;/property&gt;<br>
    --&gt;<br>
</code></pre>
so that it reads:<br>
<pre><code>    &lt;property name="parameterizedQueryFlag" value="true"&gt;&lt;/property&gt;<br>
</code></pre>
<li> If necessary (this is unlikely), uncomment the <code>minValue</code> property and supply a value other than the default value of -1.<br>
<li> Save the file.<br>
<li> In the GSA admin console, edit the connector configuration again.<br>
<li> Under <b>Disable Traversal</b>, unselect the checkbox.<br>
<li> Click the <b>Save Configuration</b> button.</blockquote>


<a href='Hidden comment: 
*eedit IbatisSqlMap.xml file to modify the replace it with the new sqlQuery value:
<pre><code>	     For Example:Modify the element as below
        <select id="getAll" resultClass="java.util.HashMap"> 
           select * from employee where emp_id between 1 and 100
         </select> 
                            TO
        <select id="getAll" resultClass="java.util.HashMap"> 
           select * from employee where emp_id between #minvalue# and #maxvalue#
        </select> 
</code></pre>

'></a>