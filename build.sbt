val emailBodyHtml = s"""
  |<html>
  |<head>
  |  <style type="text/css">
  |    /* General page style */
  |    body {
  |      font-family: Arial, sans-serif;
  |      margin: 0;
  |      padding: 0;
  |      background-color: #f9f9f9; /* Light background to contrast the table */
  |    }
  |
  |    /* Container to center content and limit width */
  |    .container {
  |      width: 100%;
  |      max-width: 600px;  /* Maximum table width */
  |      margin: 0 auto;    /* Center horizontally */
  |      background-color: #ffffff; /* White background behind the table */
  |      padding: 20px;
  |      box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1); /* Subtle shadow */
  |    }
  |
  |    h2 {
  |      color: #2E4053;
  |      margin-top: 0;
  |    }
  |
  |    p {
  |      font-size: 14px;
  |      line-height: 1.5;
  |      margin: 8px 0;
  |    }
  |
  |    table {
  |      border-collapse: collapse;
  |      width: 100%;
  |      margin-bottom: 20px;
  |      font-size: 14px;
  |    }
  |
  |    th, td {
  |      border: 1px solid #ddd;
  |      padding: 8px;
  |      text-align: left;
  |      vertical-align: middle;
  |    }
  |
  |    th {
  |      background-color: #4CAF50;
  |      color: #ffffff;
  |      font-weight: 600;
  |    }
  |
  |    .success {
  |      background-color: #d4edda;
  |      color: #155724;
  |    }
  |
  |    .failed {
  |      background-color: #f8d7da;
  |      color: #721c24;
  |    }
  |
  |    .missed {
  |      background-color: #fff3cd;
  |      color: #856404;
  |    }
  |
  |    tr:hover {
  |      background-color: #f1f1f1;
  |    }
  |  </style>
  |</head>
  |<body>
  |  <div class="container">
  |    <p>Hi Team,</p>
  |    <p>Please find below the consolidated alert report for <strong>$currentDate</strong>:</p>
  |
  |    <h2>Success Alerts</h2>
  |    <table>
  |      <tr><th>Alert Code</th></tr>
  |      ${
           if (successAlerts.isEmpty)
             "<tr><td>No successful alerts</td></tr>"
           else
             successAlerts.map(code => s"<tr class='success'><td>$code</td></tr>").mkString("\n")
         }
  |    </table>
  |
  |    <h2>Failed Alerts</h2>
  |    <table>
  |      <tr><th>Alert Code</th><th>Reason</th></tr>
  |      ${
           if (failedAlerts.isEmpty)
             "<tr><td colspan='2'>No failed alerts</td></tr>"
           else
             failedAlerts.map { case (code, reason) =>
               s"<tr class='failed'><td>$code</td><td>$reason</td></tr>"
             }.mkString("\n")
         }
  |    </table>
  |
  |    <h2>Missed Alerts (Daily & Weekly)</h2>
  |    <table>
  |      <tr><th>Alert Code</th></tr>
  |      ${
           if (missedAlerts.isEmpty)
             "<tr><td>No missed alerts</td></tr>"
           else
             missedAlerts.map(code => s"<tr class='missed'><td>$code</td></tr>").mkString("\n")
         }
  |    </table>
  |
  |    <p><strong>Cutoff Time:</strong> 4:00 PM AEST</p>
  |  </div>
  |</body>
  |</html>
""".stripMargin
