# Installs the 64-bit ODBC drivers the Windows checks read through, each a pinned installer
# checked against its SHA-256, and lists the drivers Windows then has:
#   PostgreSQL Unicode(x64)        psqlODBC 18.00.0006
#   ODBC Driver 18 for SQL Server  Microsoft's msodbcsql18 18.7.1.1 (installing it accepts Microsoft's licence)
#   MySQL ODBC 26.10 Unicode Driver MySQL Connector/ODBC 26.10.0 (its SHA-256 taken from the file
#                                  matching MySQL's published MD5, 34541f6d21bb6a4b2f56b3f8f1afcf4b)
# Usage: pwsh .github/scripts/install_odbc_drivers.ps1
$ErrorActionPreference = "Stop"

$drivers = @(
    @{
        Name = "psqlODBC"
        Uri = "https://ftp.postgresql.org/pub/odbc/releases/REL-18_00_0006/psqlodbc_x64.msi"
        Sha256 = "8962543113df83410b74f113db1ef80717b65a1dc7869e18c24bd852dd65535f"
        Arguments = @()
    },
    @{
        Name = "msodbcsql18"
        Uri = "https://go.microsoft.com/fwlink/?linkid=2378279"
        Sha256 = "21ef69e4b942f18aced55fa7c3a8d7263004e113a96690bac7509645e1b1a310"
        Arguments = @("IACCEPTMSODBCSQLLICENSETERMS=YES")
    },
    @{
        Name = "MySQL Connector/ODBC"
        Uri = "https://cdn.mysql.com/Downloads/Connector-ODBC/26.10/mysql-connector-odbc-26.10.0-winx64.msi"
        Sha256 = "ac58d7f1b4d7c1f37630a78c83c659f04c127754bfa299957b4effa32bd83cae"
        Arguments = @()
    }
)

foreach ($driver in $drivers) {
    $msi = Join-Path $env:RUNNER_TEMP ("odbc-" + [guid]::NewGuid() + ".msi")
    Invoke-WebRequest -Uri $driver.Uri -OutFile $msi
    $hash = (Get-FileHash -Algorithm SHA256 -Path $msi).Hash.ToLowerInvariant()
    if ($hash -ne $driver.Sha256) {
        throw "$($driver.Name): the installer's SHA-256 is $hash, not the pinned $($driver.Sha256); check the new release and update the pin"
    }
    $process = Start-Process msiexec.exe -ArgumentList (@("/i", $msi, "/qn", "/norestart") + $driver.Arguments) -Wait -PassThru
    if ($process.ExitCode -ne 0) {
        throw "$($driver.Name): msiexec exited $($process.ExitCode)"
    }
    Remove-Item $msi
}

Get-OdbcDriver -Platform 64-bit | Where-Object { $_.Name -match "PostgreSQL|SQL Server|MySQL" } | Format-Table Name, Platform
