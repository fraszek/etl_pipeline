<!--
  flatten_movements.xslt
  ======================
  Flattens one DB timetable / timetable-change XML file into a flat list of
  <row> elements, one per stop (<s>). Used by etl_movements_to_parquet.py.

  Input (simplified DB API format):
    <timetable station="Berlin Hbf">
      <s id="...">
        <tl c="S" n="12345" o="..."/>          trip label: category, number, owner
        <ar pt="2509051102" ct="..." cs="..."/> arrival
        <dp pt="2509051104" ct="..." cs="..."/> departure
      </s>
    </timetable>

  Output:
    <rows station="Berlin Hbf">
      <row stop_id="..." ar_pt="..." ar_ct="..." ar_cs="..." dp_pt="..." ... />
    </rows>

  Attribute meaning:
    pt = planned time, ct = changed (actual) time, cs = change status
         (c = cancelled). Timestamps are strings in YYMMDDHHmm format.
-->
<xsl:stylesheet version="1.0"
 xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:output method="xml" omit-xml-declaration="yes"/>

<!-- Root: keep the station name and process every stop element <s> -->
<xsl:template match="/">
  <rows station="{/*/@station}">
    <xsl:apply-templates select="/*/s"/>
  </rows>
</xsl:template>

<!-- One stop: copy the arrival, departure and trip-label attributes onto a flat row.
     A missing element gives an empty attribute, which the Python side reads as NULL. -->
<xsl:template match="s">
  <row
    stop_id="{@id}"

    ar_pt="{ar/@pt}"
    ar_ct="{ar/@ct}"
    ar_cs="{ar/@cs}"

    dp_pt="{dp/@pt}"
    dp_ct="{dp/@ct}"
    dp_cs="{dp/@cs}"

    tl_c="{tl/@c}"
    tl_n="{tl/@n}"
    tl_o="{tl/@o}"
  />
</xsl:template>

</xsl:stylesheet>
