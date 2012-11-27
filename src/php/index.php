<?php
$namenode_host = "analytics1010.eqiad.wmnet";
$frontend_host = "analytics1027.eqiad.wmnet";
$storm_host    = "analtyics1002.eqiad.wmnet";
?>
<html>
<head>
<!-- Bootstrap -->
    <link href="/bootstrap/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>

<div class="container">
<h1>Kraken CDH4 Links</h1>


<h2>Hadoop</h2>
<ul>
<li><a href="http://jobs.analytics.wikimedia.org/cluster">Hadoop Jobs</a> (<a href="http://<?php echo $namenode_host ?>:8088/cluster">internal</a>)</li>
<li><a href="http:///history.analytics.wikimedia.org/jobhistory">Hadoop Job History</a> (<a href="http://<?php echo $namenode_host ?>:19888/jobhistory">internal</a>)</li>
<li><a href="http://namenode.analytics.wikimedia.org/dfshealth.jsp">NameNode</a> (<a href="http://<?php echo $namenode_host ?>:50070/dfshealth.jsp">internal</a>)
</ul>

<h2>Hue</h2>
<ul>
<li><a href="http://hue.analytics.wikimedia.org/filebrowser">Hue</a> (<a href="http://<?php echo $frontend_host ?>:8888/about/">internal</a>)</li>
</ul>

<h2>Oozie</h2>
<ul>
<li><a href="http://oozie.analytics.wikimedia.org/oozie">Oozie</a> (<a href="http://<?php echo $frontend_host ?>:11000/oozie/">internal</a>)</li>
</ul>

<h2>Storm</h2>
<ul>
<li><a href="http://storm.analytics.wikimedia.org">Storm</a> (<a href="http://<?php echo $storm_host ?>:6999">internal</a>)</li>
</ul>

<strong>NOTE:</strong><br/>In order for these URLs to work, add the following line to your /etc/hosts file:
<pre>
208.80.154.154 analytics.wikimedia.org namenode.analytics.wikimedia.org jobs.analytics.wikimedia.org history.analytics.wikimedia.org oozie.analytics.wikimedia.org hue.analytics.wikimedia.org storm.analytics.wikimedia.org
</pre>
</div>

</div>

<div>

</body>