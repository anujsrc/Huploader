<map version="0.9.0">
<!-- To view this file, download free mind mapping software FreeMind from http://freemind.sourceforge.net -->
<node CREATED="1353443200916" ID="ID_1678128570" MODIFIED="1353443283856" TEXT="Huploader">
<node CREATED="1353443288533" ID="ID_580747397" MODIFIED="1353622695056" POSITION="right" TEXT="Preprocess">
<icon BUILTIN="full-2"/>
<node CREATED="1353443344949" ID="ID_421934070" MODIFIED="1353443418943" TEXT="Func: tranform xxx to CSV"/>
<node CREATED="1353443377571" ID="ID_311541975" MODIFIED="1353443483489" TEXT="Input: xxx File folder, xxx file description"/>
<node CREATED="1353443394407" ID="ID_471308761" MODIFIED="1353443414191" TEXT="Output: CSV files in a folder"/>
</node>
<node CREATED="1353443341654" ID="ID_1359272954" MODIFIED="1353443673026" POSITION="right" TEXT="Read from CSV file">
<icon BUILTIN="full-3"/>
<node CREATED="1353443422256" ID="ID_1585336430" MODIFIED="1353443464360" TEXT="Func: Read CSV file to get a record of HBase"/>
<node CREATED="1353443465506" ID="ID_686371516" MODIFIED="1353443477296" TEXT="Input: CSV file folder, CSV file description"/>
<node CREATED="1353443485763" ID="ID_1965970876" MODIFIED="1353443527230" TEXT="Output:Get list of puts Objects of HBase"/>
</node>
<node CREATED="1353443531295" ID="ID_241112485" MODIFIED="1353443676306" POSITION="right" TEXT="Write the put objects in batch to HBase">
<icon BUILTIN="full-4"/>
</node>
<node CREATED="1353443591206" ID="ID_983927869" MODIFIED="1353443663450" POSITION="left" TEXT="Partition the raw data into nodes">
<icon BUILTIN="full-1"/>
<node CREATED="1353443606272" ID="ID_1567248086" MODIFIED="1353443635565" TEXT="Input: row data location, nodes ip, location in node"/>
<node CREATED="1353443636215" ID="ID_936409597" MODIFIED="1353443651171" TEXT="Output: data is partitioned into multiple nodes"/>
</node>
</node>
</map>
