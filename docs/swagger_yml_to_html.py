#  https://gist.github.com/oseiskar/dbd51a3727fc96dcf5ed189fca491fb3
#  Copyright 2017 Otto Seiskari
#  Licensed under the Apache License, Version 2.0.
#  See http://www.apache.org/licenses/LICENSE-2.0 for the full text.
#
#  This file is based on
#  https://github.com/swagger-api/swagger-ui/blob/4f1772f6544699bc748299bd65f7ae2112777abc/dist/index.html
#  (Copyright 2017 SmartBear Software, Licensed under Apache 2.0)
#
"""
Usage:
    python swagger_yml_to_html.py < swagger_doc.yaml > index.html
"""
# pip install PyYAML
import json
import os
import sys
import yaml

TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Swagger UI</title>
  <link href="https://fonts.googleapis.com/css?family=Open+Sans:400,700|Source+Code+Pro:300,600|Titillium+Web:400,600,700" rel="stylesheet">
  <link rel="stylesheet" type="text/css" href="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/3.24.2/swagger-ui.css" >
  <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css">
  <style>
    html
    {
      box-sizing: border-box;
      overflow: -moz-scrollbars-vertical;
      overflow-y: scroll;
    }
    *,
    *:before,
    *:after
    {
      box-sizing: inherit;
    }
    body {
      margin:0;
      background: #fafafa;
    }
  </style>
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/3.24.2/swagger-ui-bundle.js"> </script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/3.24.2/swagger-ui-standalone-preset.js"> </script>
<script src="https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/js/bootstrap.min.js"></script>
<script>
window.onload = function() {
  var spec = %s;
  // Build a system
  const ui = SwaggerUIBundle({
    spec: spec,
    dom_id: '#swagger-ui',
    deepLinking: true,
    presets: [
      SwaggerUIBundle.presets.apis,
      SwaggerUIStandalonePreset
    ],
    plugins: [
      SwaggerUIBundle.plugins.DownloadUrl
    ],
    layout: "StandaloneLayout"
  })
  window.ui = ui
  
  var elements = document.getElementsByClassName("topbar");
  while(elements.length > 0){
    elements[0].parentNode.removeChild(elements[0]);
  }
  
  var bodyElement = document.getElementsByTagName("body");

  var navElm = document.createElement("nav");
  navElm.setAttribute("class", "navbar navbar-expand-lg navbar-dark bg-dark");
  navElm.innerHTML = '<a class="navbar-brand" href="..">GSP REST API</a><button class="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarNav" aria-controls="navbarNav" aria-expanded="false" aria-label="Toggle navigation"><span class="navbar-toggler-icon"></span></button><div class="collapse navbar-collapse" id="navbarNav"><ul class="navbar-nav"><li class="nav-item"><a class="nav-link" href="#">Documentation</a></li><li class="nav-item"><a class="nav-link" href="https://github.com/BYU-Hydroinformatics/gsp_rest_api">Code</a></li></ul></div>';

  bodyElement[0].insertBefore(navElm, bodyElement[0].firstChild);
}
</script>
</body>
</html>
"""

spec = yaml.safe_load(sys.stdin)
sys.stdout.write(TEMPLATE % json.dumps(spec))
with open('index.html', 'r') as idx:
    with open(os.path.join(os.path.pardir, 'GSP_API', 'templates', 'documentation.html'), 'w') as doc:
        doc.write(idx.read())
