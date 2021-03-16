Write-Information "Processing Persons"

#region Configuration
$config = ConvertFrom-Json $configuration;
$connectionString =  "DRIVER={Progress OpenEdge $($config.driver_version) driver};HOST=$($config.host_name);PORT=$($config.port);DB=$($config.database);UID=$($config.user);PWD=$($config.password);DIL=$($config.isolation_mode);AS=$($config.array_size);"

if($config.enableETWT) { $connectionString += "ETWT=1;" }
if($config.enableUWCT) { $connectionString += "UWCT=1;" }
if($config.enableKA) { $connectionString += "KA=1;" }
#endregion Configuration

#region Functions
function get_data_objects {
[cmdletbinding()]
Param (
[string]$connectionString,
[string]$query
   )
    Process
    {
        $conn = (new-object System.Data.Odbc.OdbcConnection);
        $conn.connectionstring = $connectionString;
        $conn.open();
 
        $cmd = (New-object System.Data.Odbc.OdbcCommand($query,$conn));
        $dataSet = (New-Object System.Data.DataSet);
        $dataAdapter = (New-Object System.Data.Odbc.OdbcDataAdapter($cmd));
        $dataAdapter.Fill($dataSet) | Out-Null
        $conn.Close()
 
        $result = $dataset.Tables[0];
 
        @($result);
    }
}
#endregion Functions

#region Open VPN
if($config.enableVPN) {
    Write-Information "Opening VPN"
    #Ensure VPN Connection is closed
    &"$($config.vpnClosePath)" > $null 2>&1
    
    #Reopen VPN Connection
    &"$($config.vpnOpenPath)" > $null 2>&1
}
#endregion Open VPN

#region Execute
Write-Information "Executing Staff Queries";
$staff = get_data_objects `
            -connectionString $connectionString `
            -query 'SELECT
                "NAME"."NAME-ID" 
              , "NAME"."ALTERNATE-ID"
              , "NAME"."FIRST-NAME"
              , "NAME"."MIDDLE-NAME"
              , "NAME"."LAST-NAME"
              , "NAME"."NALPHAKEY"
              , "NAME"."PRIMARY-PHONE"
              , "NAME"."SECOND-PHONE"
              , CAST("NAME"."BIRTHDATE" as date) "BIRTHDATE"
              , "NAME"."INTERNET-ADDRESS"
              , "NAME-DUSER"."DUSER-ID"
        , "STAFF-ENTITY"."ENTITY-ID"
        , "STAFF-ENTITY"."ROOM-NUMBER"
  , "STAFF-TYPE"."TYPE-STAFF-ID"
  , "STAFF"."STAFF-TITLE"
  , "STAFF"."X-TEACHER"
  , "STAFF"."X-SUBSTITUTE"
  , "STAFF"."X-COUNSELOR"
      FROM "PUB"."STAFF"
      INNER JOIN "PUB"."NAME" ON "NAME"."NAME-ID" = "STAFF"."NAME-ID"
      LEFT JOIN "PUB"."STAFF-ENTITY" "STAFF-ENTITY" ON "STAFF-ENTITY"."NAME-ID"="STAFF"."NAME-ID" AND "STAFF-ENTITY"."STATUS-CUR-YR"=''A'' AND "STAFF-ENTITY"."X-DEFAULT-ENTITY"=1
   LEFT JOIN "PUB"."STAFF-TYPE" ON "STAFF-TYPE"."NAME-ID" = "STAFF"."NAME-ID"
      LEFT JOIN "PUB"."NAME-DUSER" ON "NAME-DUSER"."NAME-ID" = "STAFF"."NAME-ID"
   WHERE "NAME"."FIRST-NAME" NOT LIKE ''ZZ%''';
 Write-Information "$($staff.count) Staff Records";


 $staffEntities  = get_data_objects `
            -connectionString $connectionString `
            -query 'SELECT * FROM "PUB"."STAFF-ENTITY"'
Write-Information "$($staffEntities.count) Staff Entity Records";

$staffDepartments = get_data_objects `
            -connectionString $connectionString `
            -query 'SELECT DISTINCT SD."X-DEFAULT-DEPT"
						        , SD."DEPARTMENT-ID"
						        , DEPT."DEPARTMENT-SDESC"
						        , DEPT."DEPARTMENT-LDESC"
						        , SD."ENTITY-ID"
						        , SD."NAME-ID"
						FROM "PUB"."STAFF-DEPARTMENT" AS SD
						INNER JOIN "PUB"."DEPARTMENT" AS DEPT ON SD."DEPARTMENT-ID" = DEPT."DEPARTMENT-ID"
						WHERE SD."SCHOOL-YEAR" = YEAR(SYSDATE())'
Write-Information "$($staffDepartments.count) Staff Department Records";

foreach($stf in ($staff | Sort-Object 'NAME-ID' -Unique))
{
    $person = @{};
    $person["ExternalId"] = $stf.'NAME-ID';
    $person["DisplayName"] = "$($stf.'FIRST-NAME') $($stf.'LAST-NAME') ($($stf.'NAME-ID'))"
    $person["Role"] = "Employee"
 
    foreach($prop in $stf.PSObject.properties)
    {
        if(@("RowError","RowState","Table","HasErrors","ItemArray") -contains $prop.Name) { continue; }
        $person[$prop.Name.replace('-','_')] = "$($prop.Value)";
    }
 
    $person["Contracts"] = [System.Collections.ArrayList]@();

    #Entities
    foreach($assign in ($staffEntities | Sort-Object 'NAME-ID',"ENTITY-ID" -Unique))
    {
        if($assign.'NAME-ID' -ne $stf.'NAME-ID') { continue; }

        $contract = @{};
        $contract["ExternalId"] = "$($stf.'NAME-ID').$($assign.'ENTITY-ID')"
        $contract["ContractType"] = "Entity"
        $contract["Title"] = "$($stf.'STAFF-TITLE')"

        foreach($prop in $assign.PSObject.properties)
        {
            if(@("RowError","RowState","Table","HasErrors","ItemArray") -contains $prop.Name) { continue; }
            $contract[$prop.Name.replace('-','_')] = "$($prop.Value)";
        }

        [void]$person.Contracts.Add($contract);
    }

    #Departments
    foreach($assign in ($staffDepartments | Sort-Object 'NAME-ID',"DEPARTMENT-SDESC" -Unique))
    {
        if($assign.'NAME-ID' -ne $stf.'NAME-ID') { continue; }

        $contract = @{};
        $contract["ExternalId"] = "$($stf.'NAME-ID').$($assign.'DEPARTMENT-SDESC')"
        $contract["ContractType"] = "Department"
        $contract["Title"] = "$($stf.'STAFF-TITLE')"

        foreach($prop in $assign.PSObject.properties)
        {
            if(@("RowError","RowState","Table","HasErrors","ItemArray") -contains $prop.Name) { continue; }
            $contract[$prop.Name.replace('-','_')] = "$($prop.Value)";
        }

        [void]$person.Contracts.Add($contract);
    }

   Write-Output ($person | ConvertTo-Json -Depth 50);
}
#endregion Execute

#region Close VPN
if($config.enableVPN) {
    Write-Information "Waiting to close VPN"
    Start-Sleep -s 15
    Write-Information "Closing VPN"
    &"$($config.vpnClosePath)" > $null 2>&1
}
#endregion Close VPN

Write-Information "Finished Processing Persons"