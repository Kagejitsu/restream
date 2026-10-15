; Inno Setup script for Restream. Build dist\ first (tools\win-bundle.ps1),
; then compile this with ISCC.exe (Inno Setup 6).
#define AppName "Restream"
#define AppVersion "0.2.0"

[Setup]
AppId={{B7F0D8F2-3C7E-4D0B-9F3A-RESTREAM0001}
AppName={#AppName}
AppVersion={#AppVersion}
DefaultDirName={autopf}\{#AppName}
DefaultGroupName={#AppName}
PrivilegesRequired=lowest
OutputDir=..\dist-installer
OutputBaseFilename=Restream-{#AppVersion}-setup
Compression=lzma2/max
SolidCompression=yes
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
UninstallDisplayIcon={app}\restream.exe

[Files]
Source: "..\dist\*"; DestDir: "{app}"; Flags: recursesubdirs ignoreversion

[Icons]
Name: "{group}\{#AppName}"; Filename: "{app}\restream.exe"
Name: "{autodesktop}\{#AppName}"; Filename: "{app}\restream.exe"; Tasks: desktopicon

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut"; GroupDescription: "Shortcuts:"

[Run]
Filename: "{app}\restream.exe"; Description: "Launch {#AppName}"; Flags: nowait postinstall skipifsilent
