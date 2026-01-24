<xsl:stylesheet version="1.0"
 xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:output method="xml" omit-xml-declaration="yes"/>

<xsl:template match="/">
  <rows station="{/*/@station}">
    <xsl:apply-templates select="/*/s"/>
  </rows>
</xsl:template>

<xsl:template match="s">
  <row
    stop_id="{@id}"

    ar_pt="{ar/@pt}"
    ar_ct="{ar/@ct}"
    ar_cs="{ar/@cs}"

    dp_pt="{dp/@pt}"
    dp_ct="{dp/@ct}"
    dp_cs="{dp/@cs}"
  />
</xsl:template>

</xsl:stylesheet>
