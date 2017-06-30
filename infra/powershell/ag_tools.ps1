Function Say-Hello{
    Param ([string]$Name)
    Write-Output "Hello $Name"
}



function Expand-ZIPFile{
    Param(
        [string]$file,
        [string]$destination)

    [System.Reflection.Assembly]::LoadWithPartialName("System.IO.Compression.FileSystem") | Out-Null
   [System.IO.Compression.ZipFile]::ExtractToDirectory($file, $destination)

}

function Test-Admin {
  $currentUser = New-Object Security.Principal.WindowsPrincipal $([Security.Principal.WindowsIdentity]::GetCurrent())
  $currentUser.IsInRole([Security.Principal.WindowsBuiltinRole]::Administrator)
}

Function Ag_copy_removeexisting_unzip_single_folder{
    Param(
        [string]$bucket_name,
        [string]$key_in_bucket,
        [string]$local_dest_path,
        [string]$s3_aws_access_key_id,
        [string]$s3_aws_secret_access_key
    )
    if ((Test-Admin) -eq $false)  {
        Write-Host "test-admin false"
    } else {
         Write-Host "test-admin true"

    }
    Copy-S3Object  -BucketName  $bucket_name -Key  $key_in_bucket -LocalFile $local_dest_path -AccessKey $s3_aws_access_key_id -SecretKey $s3_aws_secret_access_key
    if (-not (Test-Path $local_dest_path)) {
        throw "file not copied properly"
    }
    Write-Host $local_dest_path ": succesfully downloaded from s3"
    $unzipped_folder= (Get-Item  $local_dest_path).DirectoryName + "\\" + (Get-Item  $local_dest_path).BaseName
    if (Test-Path  $unzipped_folder) {
        Remove-Item $unzipped_folder -recurse  -force
    }

    if (Test-Path $unzipped_folder) {
        throw "previous unzipped folder still exists"
    }
    Write-Host $unzipped_folder ": no previous copy exists"

    $parent_unzipped_folder= (Split-Path $unzipped_folder -Parent)

    Expand-ZIPFile $local_dest_path $parent_unzipped_folder
    if (-not (Test-Path $unzipped_folder)) {
        throw " unzipped folder not present"
    }

    Remove-Item $local_dest_path -recurse
    if ( (Test-Path $local_dest_path)) {
        throw " zip file present"
    }

}

Function Ag_write_status{

    if ((Test-Admin) -eq $false)  {
        Write-Host "test-admin false"
    } else {
         Write-Host "test-admin true"

    }
    Get-ExecutionPolicy -list

}




Function Ag_removeexisting_single_folder{
    Param(
        [string]$folderpath
    )
    Ag_write_status

    if (Test-Path  $folderpath) {
        Remove-Item $folderpath -recurse  -force
    }

    if (Test-Path $folderpath) {
        throw "folder not expected to exist : " + $folderpath
    }

    Write-Host  "folder removal verified : " + $folderpath
}


Function Ag_create_if_necessary_single_folder{
    Param(
        [string]$folderpath
    )
    Ag_write_status

    if ( -not (Test-Path  $folderpath)) {
        New-Item -ItemType directory -Path $folderpath
    }

    if (-not (Test-Path  $folderpath)) {
        throw "folder expected to exist : " + $folderpath
    }

    Write-Host  "folder  existance verified : " + $folderpath
}

Function Ag_restart_computer{
    Param(
    )
    Restart-Computer -force
}



Function Ag_check_if_any_file_exists{

    Param(
        [string[]]$filenames,
        [string]$dir_to_look
    )
    $b=""
    foreach ($f in $filenames)  {
        $fullpath=Join-Path $dir_to_look $f
        if (( Test-Path $fullpath)) {
            $b=$fullpath
            break
        }
    }

    if ($b -eq "") {
        $new_exception= New-Object System.Exception ("file exist:" + $b)
        throw $new_exception
    }
}


Function Ag_check_if_file_exists{

    Param(
        [string]$filepath
    )
    if (( Test-Path $filepath)) {
        $one_or_zero=Get-Content $filepath | Out-String
        Write-Host "Exists" $one_or_zero
    } else {
        Write-Host "NotYet"
    }
}

Function Ag_throw_exception {

        Param(
            [string] $dummy
        )
        $new_exception= New-Object System.Exception ("dummy")
        throw $new_exception
}


Function Ag_force_remove_file{

    Param(
        [string[]]$filepath
    )
    if (Test-Path  $filepath) {
        Remove-Item $filepath -force
    }

    if (Test-Path  $filepath) {
        $new_exception= New-Object System.Exception ("Ag_force_reove_file:file exist:" + $filepath)
        throw $new_exception
    } else{
        Write-Host $filepath "Ag_force_reove_file:file does not exist:"
    }
}


Function Ag_writes_string_to_file{

    Param(
        [string]$filepath,
        [string]$content_string
    )
    $parent_filepath= (Split-Path $filepath -Parent)

    if (-not (Test-Path  $parent_filepath)) {
        $new_exception= New-Object System.Exception ("Ag_writes_string_to_file: parent path does not exist" + $parent_filepath)
        throw $new_exception
    } else{
        Set-Content -Value $content_string -Path $filepath
    }
    if (-not (Test-Path  $filepath)) {
        $new_exception= New-Object System.Exception ("Ag_writes_string_to_file: filepath does not exist" + $filepath)
        throw $new_exception
    }
}

Function Ag_check_how_many_processes{
Param(
        [string]$process_name
    )

    $process_check = Get-Process $process_name  -ErrorAction SilentlyContinue
    if ($process_check) {
        Write-Host  "process_name:" $process_name
        Write-Host  "process_count:"  (Get-Process -Name $process_name).Count
    } else{

        Write-Host  "process_name:" $process_name
        Write-Host  "process_count:"  0
    }
}



Function Ag_kill_processes{
Param(
        [string]$process_name
    )

    $process_check = Get-Process $process_name  -ErrorAction SilentlyContinue
    if ($process_check) {
        Stop-Process -name $process_name -force
    }
    $process_check = Get-Process $process_name  -ErrorAction SilentlyContinue
    if ($process_check) {
        $new_exception= New-Object System.Exception ("Ag_kill_processes: process still exist" + $process_name)
        throw $new_exception
    } else{

        Write-Host  "process_name:" $process_name
        Write-Host  "process_count:"  0
    }
}

Function Ag_check_current_dir{
    Param(
        [string]$expected_dir_fullpath
    )
    $current_dir=(Get-Item -Path ".\" -Verbose).FullName
    if ($expected_dir_fullpath -ne $current_dir){
        $new_exception= New-Object System.Exception ("Ag_check_current_dir: current: " + $current_dir + " expected: " + $expected_dir_fullpath)
        throw $new_exception
    } else{
        Write-Host  "process_name:" $process_name
    }
}



Function Ag_print_users_logged_on{
Param(

    )
    Write-Host (query user)
}


Function Ag_Write_To_File{
Param(
        [string]$filepath,
        [string]$content
    )
    Set-Content -path $filepath -Force -Value $content
}

Function Ag_re_pattern_remove_existing_copy{
    Param(
        [string]$pattern_re,
        [string]$remove_from_where
    )
    $objs_to_del=Get-ChildItem $remove_from_where | Where-Object {$_.Name -match $pattern_re}
    Write-Host "Hello" $objs_to_del
}