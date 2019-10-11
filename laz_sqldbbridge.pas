{ This file was automatically created by Lazarus. Do not edit!
  This source is only used to compile and install the package.
 }

unit laz_sqldbbridge;

{$warn 5023 off : no warning about unused units}
interface

uses
  usqldbvt, LazarusPackageIntf;

implementation

procedure Register;
begin
end;

initialization
  RegisterPackage('laz_sqldbbridge', @Register);
end.
