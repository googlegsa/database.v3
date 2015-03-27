This document gives detail steps for changing Display URL using GSA front-end Stylesheet. In some cases database Connector uses pseudo display URL for its records. Connector admin can change this hypothetical display URL using from-end Stylesheet of GSA.

**Step 1:-**

Locate the front-end Stylesheet you want to alter on Serving > Front Ends page.


**Step 2:-**

Find the following section of the default stylesheet

<!-- A single result (do not customize) -->

**Step 3:-**

In that section, find the following line:


For 5.0 XSLT Version
```
<xsl:variable name="display_url_tmp" select="substring-after(UD, ':')"/> 
```

For 6.0 XSLT Version
```
<xsl:variable name="display_url1" select="substring-after(UD, '://')"/>
```

**Step 4:-**

Replace that line with the following. Make sure to add your own values for $rewrite\_from and $rewrite\_to.
For display URL for your database connector configuration check feed log file.

For 5.0 XSLT Version
```
<xsl:variable name="rewrite_from" select="'dbconnector://pseudo display URL'" />
<xsl:variable name="rewrite_to" select="'http://actual-document-url/'"/>
<xsl:variable name="rewritten_url">
<xsl:choose>
<xsl:when test="(substring-before(U, $rewrite_from) = '' ) and contains(U, $rewrite_from)">
<xsl:value-of select="concat($rewrite_to, substring-after(U, $rewrite_from))"/>
</xsl:when>
<xsl:otherwise>
<xsl:value-of select="U"/>
</xsl:otherwise>
</xsl:choose>
</xsl:variable>
<xsl:variable name="display_url_tmp" select="substring-after($rewritten_url, '//')"/>
```

For 6.0 XSLT Version
```
<xsl:variable name="rewrite_from" select="'dbconnector://pseudo display URL/'" />
<xsl:variable name="rewrite_to" select="'http://actual-document-url/'"/>
<xsl:variable name="rewritten_url">
<xsl:choose>
<xsl:when test="(substring-before(U, $rewrite_from) = '' ) and contains(U, $rewrite_from)">
<xsl:value-of select="concat($rewrite_to, substring-after(U, $rewrite_from))"/>
</xsl:when>
<xsl:otherwise>
<xsl:value-of select="U"/>
</xsl:otherwise>
</xsl:choose>
</xsl:variable>
<xsl:variable name="display_url1" select="substring-after($rewritten_url, '://')"/>
```

**Step 5:-**

Look for the following line (about 70 lines down from above)
```
<xsl:otherwise>
<xsl:value-of disable-output-escaping='yes' select="U"/>
</xsl:otherwise>
```

**Step 6:-**

Change it to the following.
```
<xsl:otherwise>
<xsl:value-of disable-output-escaping='yes' select="$rewritten_url"/>
</xsl:otherwise>
```

**Step 7:-**
Save this new stylesheet and test it.