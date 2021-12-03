unit superdateNoWinapi;

{$IFDEF FPC}
  {$MODE OBJFPC}{$H+}
{$ENDIF}

interface
uses
  supertypes;

function JavaToDelphiDateTime(const dt: Int64): TDateTime;
function DelphiToJavaDateTime(const dt: TDateTime): Int64;

function DelphiDateTimeToISO8601Date(dt: TDateTime): SOString;

function ISO8601DateToJavaDateTime(const str: SOString; var ms: Int64): Boolean;
function ISO8601DateToDelphiDateTime(const str: SOString; var dt: TDateTime): Boolean;

implementation

uses
  SysUtils, DateUtils, Math;

function JavaToDelphiDateTime(const dt: Int64): TDateTime;
//var utc, local: TDateTime;
begin
  Result := UnixToDateTime(dt);
end;

function DelphiToJavaDateTime(const dt: TDateTime): Int64;
//var local, utc, st: TDateTime;
begin
  Result := DateTimeToUnix(dt)
end;

function DelphiDateTimeToISO8601Date(dt: TDateTime): SOString;
begin
  Result := DateToISO8601(dt);
end;

{ iso -> java }

function ISO8601DateToJavaDateTime(const str: SOString; var ms: Int64): Boolean;
begin
  ms := DateTimeToMilliseconds(ISO8601ToDate(str));
  Result := True;
end;

function ISO8601DateToDelphiDateTime(const str: SOString; var dt: TDateTime): Boolean;
//var ms: Int64;
begin
  Result := TryISO8601ToDate(str, dt);
end;

end.
