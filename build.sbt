while (masterTable2_format.next()) {
  // Quote fields inline if they contain commas
  val Master2TransformedRow = s"${if (masterTable2_format.getString(1).contains(",")) "\"" + masterTable2_format.getString(1) + "\"" else masterTable2_format.getString(1)}," +
                              s"${if (masterTable2_format.getString(2).contains(",")) "\"" + masterTable2_format.getString(2) + "\"" else masterTable2_format.getString(2)}," +
                              s"${if (masterTable2_format.getString(3).contains(",")) "\"" + masterTable2_format.getString(3) + "\"" else masterTable2_format.getString(3)}," +
                              s"${if (masterTable2_format.getString(4).contains(",")) "\"" + masterTable2_format.getString(4) + "\"" else masterTable2_format.getString(4)}"

  MasterTable2DF += Master2TransformedRow
}
