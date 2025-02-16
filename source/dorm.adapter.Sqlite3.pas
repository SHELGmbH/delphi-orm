{ *******************************************************************************
  Copyright 2010-2016 Daniele Teti

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
  ******************************************************************************** }
unit dorm.adapter.Sqlite3;

interface

uses
  dorm.Commons,
  dorm.Mappings,
  classes,
  SysUtils,
  DB,
  Sqlite3,
  Sqlite3udf,
  SQLiteWrap,
  dorm.Filters,
  Rtti,
  dorm,
  superobject,
  TypInfo,
  FMTBcd,
  Generics.Collections,
  dorm.Collections,
  dorm.adapter.Base,
  dorm.Mappings.Strategies;

type
  TSqlite3PersistStrategy = class(TBaseAdapter, IdormPersistStrategy)
  strict private
    function GetSQlite3ReaderFor(ARttiType: TRttiType; AMappingTable: TMappingTable;
      const Value: TValue; AMappingRelationField: TMappingField = nil): TSqliteTable;
  protected
    FFormatSettings: TFormatSettings;
    DB: TSQLiteDatabase;
    FLogger: IdormLogger;
    FKeyType: TdormKeyType;
    FNullKeyValue: TValue;
    FLastInsertOID: TValue;
    FStandardDateFormat: string;
    procedure InitFormatSettings;
    function CreateObjectFromSqliteTable(ARttiType: TRttiType; AReader: TSqliteTable;
      AMappingTable: TMappingTable): TObject;
    procedure LoadObjectFromSqliteTable(AObject: TObject; ARttiType: TRttiType;
      AReader: TSqliteTable; AFieldsMapping: TMappingFieldList);
    function GetLogger: IdormLogger;
    function GetSqlite3ParameterValue(aFieldType: string; aParameterName: String; aValue: TValue): string;
    procedure SetSqlite3ParameterValue(ADB: TSQLiteDatabase; aFieldType: string;
      aParameterName: String; aValue: TValue);
  public
    function CanUpsert: Boolean; override;
    function FillPrimaryKeyParam(ADB: TSQLiteDatabase; AParamName: String; const Value: TValue)
      : TValue; overload;
    function EscapeString(const Value: String): String; override;
    function EscapeDate(const Value: TDate): String; override;
    function EscapeDateTime(const Value: TDate; AWithMillisSeconds : boolean = false): String; override;
    function GetLastInsertOID: TValue;
    function GetKeysGenerator: IdormKeysGenerator;
    function Insert(ARttiType: TRttiType; AObject: TObject; AMappingTable: TMappingTable): TValue; overload;
    function Insert(ARttiType: TRttiType; AObject: TObject; AMappingTable: TMappingTable; AUpsert: Boolean = False): TValue; overload;
    function Upsert(ARttiType: TRttiType; AObject: TObject; AMappingTable: TMappingTable): TValue; override;
    function Update(ARttiType: TRttiType; AObject: TObject; AMappingTable: TMappingTable;
      ACurrentVersion: Int64): Int64;
    function Delete(ARttiType: TRttiType; AObject: TObject; AMappingTable: TMappingTable;
      ACurrentVersion: Int64): Int64;
    procedure DeleteAll(AMappingTable: TMappingTable);
    function Count(AMappingTable: TMappingTable): Int64;
    function Load(ARttiType: TRttiType; AMappingTable: TMappingTable; const Value: TValue;
      AObject: TObject): Boolean; overload;
    function Load(ARttiType: TRttiType; AMappingTable: TMappingTable;
      AMappingRelationField: TMappingField; const Value: TValue; AObject: TObject; const DontRaiseExceptionOnUnexpectedMultiRowResult: Boolean): Boolean; overload;
    function List(ARttiType: TRttiType; AMappingTable: TMappingTable; ACriteria: ICriteria)
      : TObjectList<TObject>;
    procedure LoadList(AList: TObject; ARttiType: TRttiType; AMappingTable: TMappingTable;
      ACriteria: ICriteria); overload;
    procedure ConfigureStrategy(ConfigurationInfo: ISuperObject; const UnicodeDB: Boolean); virtual;
    procedure InitStrategy;
    procedure StartTransaction;
    procedure Commit;
    procedure Rollback;
    function InTransaction: Boolean;
    procedure SetLogger(ALogger: IdormLogger);
    destructor Destroy; override;
    class procedure register;
    function IsNullKey(ATableMap: TMappingTable; const Value: TValue; ACheckFK: Boolean = True): Boolean;
    function GetNullKeyValue: TValue;
    function GetKeyType: TdormKeyType;
    function RawExecute(SQL: string): Int64;
    function ExecuteAndGetFirst(SQL: string): Int64;
    function GetDatabaseBuilder(AEntities: TList<String>; AMappings: ICacheMappingStrategy)
      : IDataBaseBuilder;
    function ExecuteCommand(ACommand: IdormCommand): Int64;
    procedure ExecStoredProcedure(const AProcName: String; _InputParams, _OutputParams: TStringList);
    function GetConnection : TCustomConnection;
    function GetFieldTypeAndSize(const _Table, _Field: string; out _Type: TFieldType; out _Size: integer): Boolean;
    function GetDBName: string;
    function GetEngine: string;
  end;

implementation

uses
  dorm.Utils, System.StrUtils, FireDAC.Stan.Consts, System.Types;

function TSqlite3PersistStrategy.Update(ARttiType: TRttiType; AObject: TObject;
  AMappingTable: TMappingTable; ACurrentVersion: Int64): Int64;
var
  field: TMappingField;
  SQL: string;
  // I, pk_idx: Integer;
  v: TValue;
  sql_fields_names: string;
  pk_field: string;
begin
  sql_fields_names := '';
  for field in AMappingTable.Fields do
    if not field.IsPK then
      sql_fields_names := sql_fields_names + ',"' + field.FieldName + '" = :' + field.FieldName;
  System.Delete(sql_fields_names, 1, 1);
  pk_field := AMappingTable.Fields[GetPKMappingIndex(AMappingTable.Fields)].FieldName;
  SQL := Format('UPDATE %S SET %S WHERE "%S" = :%S', [AMappingTable.TableName, sql_fields_names,
    pk_field, pk_field]);
  GetLogger.Debug(AMappingTable.Fields[GetPKMappingIndex(AMappingTable.Fields)].FieldName);
  DB.ParamsClear;
  if ACurrentVersion > 0 then
    SQL := SQL + ' AND OBJVERSION = ' + IntToStr(ACurrentVersion);
  GetLogger.Debug('PREPARING: ' + SQL);
  for field in AMappingTable.Fields do
  begin
    v := TdormUtils.GetField(AObject, field.RTTICache);
    SetSqlite3ParameterValue(DB, field.FieldType, ':' + field.FieldName, v);
  end;
  GetLogger.Debug('EXECUTING PREPARED: ' + SQL);
  DB.ExecSQL(SQL);
  Result := DB.LastChangedRows;
  DB.ParamsClear;
end;

function TSqlite3PersistStrategy.Upsert(ARttiType: TRttiType; AObject: TObject;
  AMappingTable: TMappingTable): TValue;
var field: TMappingField;
    v: TValue;
    Upsert: Boolean;
    Version: string;
    VersionArr: TArray<string>;
    VersionOK: Boolean;
begin
  Upsert := True;
  VersionOK := True;
  for field in AMappingTable.Fields do begin
    if field.IsPK then begin
      v := TdormUtils.GetField(AObject, field.RTTICache);
      if IsNullKey(AMappingTable, v, False) then begin
        Upsert := False;
        Break;
      end;
    end;
  end;
  if Upsert then begin
    VersionOK := False;
    Version := DB.Version;
    VersionArr := Version.Split(['.']);
    if Length(VersionArr) >= 2 then begin
      if (StrToIntDef(VersionArr[0], 0) >= 3) and (StrToIntDef(VersionArr[1], 0) >= 24) then begin
        VersionOK := True;
      end;
    end;
  end;
  if VersionOK OR not Upsert then begin
    Insert(ARttiType, AObject, AMappingTable, Upsert);
  end else begin
    Update(ARttiType, AObject, AMappingTable, 0);
  end;
end;

function TSqlite3PersistStrategy.CanUpsert: Boolean;
begin
  Result := True;
end;

procedure TSqlite3PersistStrategy.Commit;
begin
  if not DB.InTransaction then
    raise Exception.Create('Transaction is not active');
  DB.Commit('trans');
end;

procedure TSqlite3PersistStrategy.ConfigureStrategy(ConfigurationInfo: ISuperObject; const UnicodeDB: Boolean);
var
  database_connection_string: string;
begin
  InitFormatSettings;
  database_connection_string := ConfigurationInfo.S['database_connection_string'];
  // Do not support password. The configuration about "password" is ingored
  // password := ConfigurationInfo.S['password'];
  DB := TSQLiteDatabase.Create(database_connection_string);
  // Do not support String Keys Generator. The configuration about "keys_generator" is ingored
  // FKeysGeneratorClassName := ConfigurationInfo.S['keys_generator'];
  // t := ctx.FindType(FKeysGeneratorClassName);
  // if t = nil then
  // raise EdormException.Create('Unknown key generator ' +
  // FKeysGeneratorClassName);
  // obj := t.AsInstance.MetaclassType.Create;
  // if not Supports(obj, IdormKeysGenerator, FKeysGenerator) then
  // raise EdormException.Create('Keys generator ' + FKeysGeneratorClassName +
  // ' doesn''t implements ''IdormKeysGenerator''');
  // FKeysGenerator.SetPersistStrategy(self);
  // self._Release;
  FStandardDateFormat := ConfigurationInfo.S['standard_datetime_format'];
  if (SameText(ConfigurationInfo.S['key_type'], 'integer')) then
  begin
    FKeyType := ktInteger;
    FNullKeyValue := ConfigurationInfo.I['null_key_value']
  end
  else if (SameText(ConfigurationInfo.S['key_type'], 'string')) then
  begin
    FKeyType := ktString;
    FNullKeyValue := ConfigurationInfo.S['null_key_value']
  end
  else
    raise EdormException.Create('Unknown key type');
  inherited;
end;

function TSqlite3PersistStrategy.Count(AMappingTable: TMappingTable): Int64;
var
  SQL: string;
  Table: TSqliteTable;
begin
  // Result := -1;
  SQL := 'SELECT COUNT(*) FROM ' + AMappingTable.TableName;
  GetLogger.Debug('PREPARING: ' + SQL);
  Table := DB.GetTable(SQL);
  try
    Result := Table.FieldAsInteger(0);
  finally
    Table.Free;
  end;
end;

function TSqlite3PersistStrategy.Delete(ARttiType: TRttiType; AObject: TObject;
  AMappingTable: TMappingTable; ACurrentVersion: Int64): Int64;
var
  pk_idx: Integer;
  pk_value: TValue;
  pk_attribute_name, pk_field_name, SQL: string;
begin
  pk_idx := GetPKMappingIndex(AMappingTable.Fields);
  if pk_idx = -1 then
    raise Exception.Create('Invalid primary key for table ' + AMappingTable.TableName);
  pk_attribute_name := AMappingTable.Fields[pk_idx].name;
  pk_field_name := AMappingTable.Fields[pk_idx].FieldName;
  pk_value := ARttiType.GetProperty(pk_attribute_name).GetValue(AObject);
  SQL := 'DELETE FROM ' + AnsiQuotedStr(AMappingTable.TableName, '"') + ' WHERE ' +
    AnsiQuotedStr(pk_field_name, '"') + ' = :' + pk_field_name;
  if ACurrentVersion > 0 then // optlock
    SQL := SQL + ' AND OBJVERSION = ' + IntToStr(ACurrentVersion);
  GetLogger.Debug('PREPARING: ' + SQL);
  DB.ParamsClear;
  FillPrimaryKeyParam(DB, ':' + pk_field_name, pk_value);
  GetLogger.Debug('EXECUTING PREPARED: ' + SQL);
  DB.ExecSQL(SQL);
  Result := DB.LastChangedRows;
  DB.ParamsClear;
end;

procedure TSqlite3PersistStrategy.DeleteAll(AMappingTable: TMappingTable);
var
  SQL: string;
begin
  SQL := 'DELETE FROM ' + AnsiQuotedStr(AMappingTable.TableName, '"');
  GetLogger.Debug('EXECUTING :' + SQL);
  DB.ExecSQL(SQL);
end;

destructor TSqlite3PersistStrategy.Destroy;
begin
  DB.Free;
  inherited;
end;

function TSqlite3PersistStrategy.EscapeDate(const Value: TDate): String;
begin
  Result := FormatDateTime('YYYY-MM-DD', Value);
end;

function TSqlite3PersistStrategy.EscapeDateTime(const Value: TDate; AWithMillisSeconds : boolean = false): String;
begin
  if FStandardDateFormat = '' then begin
    Result := FormatDateTime('YYYY-MM-DD HH:NN:SS' + IfThen(AWithMillisSeconds, '.ZZZ'), Value);
  end else begin
    Result := FormatDateTime(FStandardDateFormat + IfThen(AWithMillisSeconds, '.ZZZ'), Value);
  end;
end;

function TSqlite3PersistStrategy.EscapeString(const Value: String): String;
begin
  Result := StringReplace(Value, '''', '''''', [rfReplaceAll]);
end;

procedure TSqlite3PersistStrategy.ExecStoredProcedure(const AProcName: String;
  _InputParams, _OutputParams: TStringList);
begin
  inherited;
//
end;

function TSqlite3PersistStrategy.ExecuteAndGetFirst(SQL: string): Int64;
begin
  Result := DB.GetTableValue(SQL);
end;

function TSqlite3PersistStrategy.ExecuteCommand(ACommand: IdormCommand): Int64;
var
  SQL: string;
  ts : TDateTime;
begin
  SQL := ACommand.GetSQL;
  GetLogger.Debug('EXECUTING: ' + SQL);
  ts := Now;
  try
    Result := RawExecute(SQL);
    GetLogger.LogCall(SQL, ts);
  except
    on e : Exception do begin
      GetLogger.Error(e.Message, SQL);
      raise;
    end;
  end;
//  raise EdormException.Create('not implemented');
end;

function TSqlite3PersistStrategy.GetConnection: TCustomConnection;
begin
  Result := nil;
end;

function TSqlite3PersistStrategy.GetDatabaseBuilder(AEntities: TList<String>;
  AMappings: ICacheMappingStrategy): IDataBaseBuilder;
begin
  raise EdormException.Create('Not implemented');
  // Result := TdormSqlite3DBCreator.Create(AMappings, AEntities);
end;

function TSqlite3PersistStrategy.GetDBName: string;
begin
  Result := '';
end;

function TSqlite3PersistStrategy.GetEngine: string;
begin
  Result := S_FD_SQLite_RDBMS;
end;

function TSqlite3PersistStrategy.GetFieldTypeAndSize(const _Table,
  _Field: string; out _Type: TFieldType; out _Size: integer): Boolean;
begin
  Result := False;
end;

function TSqlite3PersistStrategy.GetKeysGenerator: IdormKeysGenerator;
begin
  raise EdormException.Create('Not supported on ' + ClassName);
end;

function TSqlite3PersistStrategy.GetKeyType: TdormKeyType;
begin
  Result := FKeyType;
end;

function TSqlite3PersistStrategy.GetLastInsertOID: TValue;
begin
  Result := FLastInsertOID;
end;

function TSqlite3PersistStrategy.GetLogger: IdormLogger;
begin
  Result := FLogger;
end;

procedure TSqlite3PersistStrategy.InitFormatSettings;
begin
  FFormatSettings.LongDateFormat := 'YYYY-MM-DD';
  FFormatSettings.ShortDateFormat := 'YYYY-MM-DD';
  FFormatSettings.LongTimeFormat := 'HH:NN:SS';
  FFormatSettings.ShortTimeFormat := 'HH:NN:SS';
  FFormatSettings.DateSeparator := '-';
  FFormatSettings.TimeSeparator := ':';
end;

procedure TSqlite3PersistStrategy.InitStrategy;
begin
  FLastInsertOID := TValue.Empty;
end;

function TSqlite3PersistStrategy.Insert(ARttiType: TRttiType; AObject: TObject;
  AMappingTable: TMappingTable): TValue;
begin
  Insert(ARttiType, AObject, AMappingTable, False);
end;

function TSqlite3PersistStrategy.Insert(ARttiType: TRttiType; AObject: TObject;
  AMappingTable: TMappingTable; AUpsert: Boolean): TValue;
var
  field: TMappingField;
  sql_fields_names, sql_fields_values, SQL: ansistring;
  pk_idx: Integer;
  pk_idxs: TIntegerDynArray;
  pk_names: string;
  I: Integer;
  v, pk_value: TValue;
  PreStr: string;
  OldLastInsertedRowID: Integer;
  NewLastInsertedRowID: Integer;
begin
  sql_fields_names := '';
  for field in AMappingTable.Fields do
    if (not field.IsPK or field.IsFK or AUpsert) then
      sql_fields_names := sql_fields_names + ',' + ansistring(field.FieldName) + '';
  System.Delete(sql_fields_names, 1, 1);
  sql_fields_values := '';
  for field in AMappingTable.Fields do
    if (not field.IsPK or field.IsFK or AUpsert) then
      sql_fields_values := sql_fields_values + ', :' + ansistring(field.FieldName);
  System.Delete(sql_fields_values, 1, 1);
  SQL := ansistring(Format('INSERT INTO %s (%S) VALUES (%S) ', [AMappingTable.TableName,
    sql_fields_names, sql_fields_values]));
  pk_idx := GetPKMappingIndex(AMappingTable.Fields);
  pk_idxs := GetPKMappingIndexes(AMappingTable.Fields);
  pk_names := '';
  for I := 0 to Length(pk_idxs) - 1 do begin
    pk_names := pk_names + IfThen(pk_names <> '', ',') + AMappingTable.Fields[pk_idxs[I]].FieldName;
  end;
  if AUpsert then begin
    SQL := SQL + ansistring(Format('ON CONFLICT(%s) DO UPDATE SET ', [pk_names]));
    PreStr := '';
    for field in AMappingTable.Fields do begin
      if not field.IsPK then begin
        SQL := SQL + ansistring(PreStr + Format('%s = excluded.%s', [field.name, field.name]));
        PreStr := ',';
      end;
    end;
  end;
  GetLogger.Debug('PREPARING :' + string(SQL));
  DB.ParamsClear;
    for field in AMappingTable.Fields do
    begin
      v := TdormUtils.GetField(AObject, field.RTTICache);
      if (not field.IsPK or field.IsFK or AUpsert) then
        SetSqlite3ParameterValue(DB, field.FieldType, ':' + field.FieldName, v);
    end;
  GetLogger.Debug('EXECUTING PREPARED :' + string(SQL));
  OldLastInsertedRowID := DB.LastInsertRowID;
  DB.SetLastInsertRowID(0);
  DB.ExecSQL(string(SQL));
  NewLastInsertedRowID := DB.LastInsertRowID;
  if NewLastInsertedRowID = 0 then begin
    DB.SetLastInsertRowID(OldLastInsertedRowID);
    pk_value := TdormUtils.GetField(AObject, AMappingTable.Fields[pk_idx].RTTICache);
  end else begin
    pk_value := NewLastInsertedRowID;
  end;
  if AMappingTable.Fields[pk_idx].FieldType = 'string' then begin
    pk_value := pk_value.ToString;
  end;
  if pk_idx <> -1 then begin
    TdormUtils.SetProperty(AObject, AMappingTable.Fields[pk_idx].RTTICache, pk_value);
  end;
  Result := pk_value;
  FLastInsertOID := Result;
end;

function TSqlite3PersistStrategy.InTransaction: Boolean;
begin
  Result := DB.InTransaction;
end;

function TSqlite3PersistStrategy.IsNullKey(ATableMap: TMappingTable; const Value: TValue; ACheckFK: Boolean = True): Boolean;
begin
  if not Assigned(AtableMap.Id) then begin
    Result := True;
  end else if ACheckFK and ATableMap.Id.IsFK then begin
    Result := True;
  end else begin
    if ATableMap.Id.FieldType = 'string' then begin
      Result := TdormUtils.EqualValues(Value, '');
    end else begin
      Result := TdormUtils.EqualValues(Value, FNullKeyValue);
    end;
  end;
end;

function TSqlite3PersistStrategy.GetNullKeyValue: TValue;
begin
  Result := FNullKeyValue;
end;

function TSqlite3PersistStrategy.List(ARttiType: TRttiType; AMappingTable: TMappingTable;
  ACriteria: ICriteria): TObjectList<TObject>;
begin
  Result := NewList();
  LoadList(Result, ARttiType, AMappingTable, ACriteria);
end;

function TSqlite3PersistStrategy.Load(ARttiType: TRttiType; AMappingTable: TMappingTable;
  AMappingRelationField: TMappingField; const Value: TValue; AObject: TObject; const DontRaiseExceptionOnUnexpectedMultiRowResult: Boolean): Boolean;
var
  reader: TSqliteTable;
begin
  reader := GetSQlite3ReaderFor(ARttiType, AMappingTable, Value, AMappingRelationField);
  try
    Result := not reader.Eof;
    if Result then
      LoadObjectFromSqliteTable(AObject, ARttiType, reader, AMappingTable.Fields);
  finally
    reader.Free;
  end;
end;

procedure TSqlite3PersistStrategy.LoadList(AList: TObject; ARttiType: TRttiType;
  AMappingTable: TMappingTable; ACriteria: ICriteria);
var
  SQL: string;
  reader: TSqliteTable;
  CustomCriteria: ICustomCriteria;
  // obj: TObject;
  v: TValue;
begin
  if Assigned(ACriteria) and TInterfacedObject(ACriteria).GetInterface(ICustomCriteria,
    CustomCriteria) then
    SQL := CustomCriteria.GetSQL
  else
    SQL := Self.GetSelectSQL(ACriteria, AMappingTable);
  GetLogger.Debug('EXECUTING: ' + SQL);
  reader := DB.GetTable(SQL, false);
  try
    while not reader.Eof do
    begin
      v := CreateObjectFromSqliteTable(ARttiType, reader, AMappingTable);
      TdormUtils.MethodCall(AList, 'Add', [v]);
      reader.Next;
    end;
  finally
    reader.Free;
  end;
end;

function TSqlite3PersistStrategy.FillPrimaryKeyParam(ADB: TSQLiteDatabase; AParamName: String;
  const Value: TValue): TValue;
begin
  try
    case FKeyType of
      ktString:
        begin
          { todo: implement string primary keys for sqlite3 }
//          raise EdormException.Create(ClassName + ' do not support string primary keys');
          ADB.AddParamText(AParamName, Value.AsString);
          Result := Value.AsString;
        end;
      ktInteger:
        begin
          ADB.AddParamInt(AParamName, Value.AsInteger);
          Result := Value.AsInteger;
        end;
    end;
  except
    on E: Exception do
      raise EdormException.Create('Error during fill primary key for query. ' + E.Message);
  end;
end;

function TSqlite3PersistStrategy.GetSqlite3ParameterValue(aFieldType,
  aParameterName: String; aValue: TValue): string;
var
  sourceStream: TStream;
//  str: TMemoryStream;
begin
  if CompareText(aFieldType, 'string') = 0 then
  begin
    Result := QuotedStr(aValue.AsString);
  end
  else if (CompareText(aFieldType, 'integer') = 0) or (CompareText(aFieldType, 'int64') = 0) then
  begin
    Result := IntToStr(aValue.AsInteger);
  end
  else if CompareText(aFieldType, 'decimal') = 0 then
  begin
    Result := FloatToStr(aValue.AsExtended);
  end
  else if CompareText(aFieldType, 'boolean') = 0 then
  begin
    if aValue.AsBoolean then
    begin
      Result := '1';
    end
    else
    begin
      Result := '0';
    end;
  end
  else if CompareText(aFieldType, 'date') = 0 then
  begin
    Result := QuotedStr(ISODateToString(aValue.AsExtended));
  end
  else if CompareText(aFieldType, 'datetime') = 0 then
  begin
    if aValue.AsExtended = 0 then begin
      Result := 'NULL';
    end else begin
      if FStandardDateFormat = '' then begin
        Result := QuotedStr(ISODateTimeToString(aValue.AsExtended));
      end else begin
        Result := QuotedStr(FormatDateTime(FStandardDateFormat, aValue.AsExtended, TFormatSettings.Create));
      end;
    end;
  end
  else if CompareText(aFieldType, 'time') = 0 then
  begin
    Result := QuotedStr(ISOTimeToString(Frac(FloatToDateTime(aValue.AsExtended))));
  end
  else if CompareText(aFieldType, 'blob') = 0 then
  begin
    sourceStream := TStream(aValue.AsObject);
    if sourceStream = nil then
    begin
      Result := 'NULL';
    end
    else
    begin
      raise Exception.Create('blob-type not implemented (' + aParameterName + ')');
//      str := TMemoryStream.Create;
//      try
//        sourceStream.Position := 0;
//        str.CopyFrom(sourceStream, sourceStream.Size);
//        str.Position := 0;
//        ADB.AddParamBlobPtr(aParameterName, str.Memory, str.Size);
//      finally
//        str.Free;
//      end;
    end;
  end;
end;

function TSqlite3PersistStrategy.GetSQlite3ReaderFor(ARttiType: TRttiType;
  AMappingTable: TMappingTable; const Value: TValue; AMappingRelationField: TMappingField)
  : TSqliteTable;
var
  pk_idx: Integer;
  pk_attribute_name, pk_field_name, SQL: string;
begin
  if AMappingRelationField = nil then
  begin
    pk_idx := GetPKMappingIndex(AMappingTable.Fields);
    if pk_idx = -1 then
      raise Exception.Create('Invalid primary key for table ' + AMappingTable.TableName);
    pk_attribute_name := AMappingTable.Fields[pk_idx].name;
    pk_field_name := AMappingTable.Fields[pk_idx].FieldName;
    SQL := 'SELECT ' + GetSelectFieldsList(AMappingTable.Fields, true) + ' FROM ' +
      AMappingTable.TableName + ' WHERE ' + pk_field_name + ' = :' + pk_field_name;
  end
  else
  begin
    pk_idx := GetPKMappingIndex(AMappingTable.Fields);
    if pk_idx = -1 then
      raise Exception.Create('Invalid primary key for table ' + AMappingTable.TableName);
    pk_field_name := AMappingTable.Fields[pk_idx].FieldName;
    SQL := 'SELECT ' + GetSelectFieldsList(AMappingTable.Fields, true) + ' FROM ' +
      AMappingTable.TableName + ' WHERE ' + AMappingRelationField.FieldName + ' = :' +
      pk_field_name;
  end;
  GetLogger.Debug('PREPARING: ' + SQL);
  DB.ParamsClear;
  FillPrimaryKeyParam(DB, ':' + pk_field_name, Value);
  GetLogger.Debug('EXECUTING PREPARED: ' + SQL);
  Result := DB.GetTable(SQL);
end;

function TSqlite3PersistStrategy.Load(ARttiType: TRttiType; AMappingTable: TMappingTable;
  const Value: TValue; AObject: TObject): Boolean;
var
  reader: TSqliteTable;
begin
  reader := GetSQlite3ReaderFor(ARttiType, AMappingTable, Value);
  try
    Result := not reader.Eof;
    if Result then
      LoadObjectFromSqliteTable(AObject, ARttiType, reader, AMappingTable.Fields);
  finally
    reader.Free;
  end;
end;

procedure TSqlite3PersistStrategy.LoadObjectFromSqliteTable(AObject: TObject; ARttiType: TRttiType;
  AReader: TSqliteTable; AFieldsMapping: TMappingFieldList);
var
  field: TMappingField;
  v: TValue;
  S: string;
  sourceStream: TStream;
begin
  try
    for field in AFieldsMapping do
    begin
      if CompareText(field.FieldType, 'string') = 0 then
      begin
        v := AReader.FieldAsString(AReader.FieldIndex[field.FieldName]);
        S := field.FieldName + ' as string';
      end
      else if (CompareText(Field.FieldType, 'integer') = 0) or (CompareText(Field.FieldType, 'int64') = 0) then
      begin
        v := AReader.FieldAsInteger(AReader.FieldIndex[field.FieldName]);
        S := field.FieldName + ' as integer';
      end
      else if CompareText(field.FieldType, 'blob') = 0 then
      begin
        // targetStream := nil;
        sourceStream := nil;
        if not AReader.FieldIsNull(AReader.FieldIndex[field.FieldName]) then
          sourceStream := AReader.FieldAsBlob(AReader.FieldIndex[field.FieldName]);
        S := field.FieldName + ' as blob';
        if Assigned(sourceStream) then
        begin
          sourceStream.Position := 0;
          v := sourceStream;
        end
        else
          v := nil;
      end
      else if CompareText(field.FieldType, 'decimal') = 0 then
      begin
        v := AReader.FieldAsDouble(AReader.FieldIndex[field.FieldName]);
        S := field.FieldName + ' as decimal';
      end
      else if CompareText(field.FieldType, 'boolean') = 0 then
      begin
        v := AReader.FieldAsInteger(AReader.FieldIndex[field.FieldName]);
        v := v.AsInteger = 1;
        S := field.FieldName + ' as boolean';
      end
      else if CompareText(field.FieldType, 'datetime') = 0 then
      begin
        v := AReader.FieldAsString(AReader.FieldIndex[field.FieldName]);
        v := ISOStrToDateTime(v.AsString);
        S := field.FieldName + ' as datetime';
      end
      else if CompareText(field.FieldType, 'time') = 0 then
      begin
        v := AReader.FieldAsString(AReader.FieldIndex[field.FieldName]);
        v := ISOStrToTime(v.AsString);
        S := field.FieldName + ' as time';
      end
      else if CompareText(field.FieldType, 'date') = 0 then
      begin
        v := AReader.FieldAsString(AReader.FieldIndex[field.FieldName]);
        v := ISOStrToDate(v.AsString);
        S := field.FieldName + ' as date';
      end
      else
        raise Exception.Create('Unknown field type for ' + field.FieldName);
      try
        TdormUtils.SetField(AObject, field.RTTICache, v);
      except
        on E: Exception do
        begin
          raise EdormException.Create(E.Message + sLineBreak + '. Probably cannot write ' +
            ARttiType.ToString + '.' + S);
        end;
      end;
    end;
  except
    on E: Exception do
    begin
      raise;
    end;
  end;
end;

// function TSqlite3PersistStrategy.LoadObjectFromDBXReader(ARttiType: TRttiType;
// AReader: TDBXReader; AFieldsMapping: TMappingFieldList;
// AObject: TObject): Boolean;
// var
// obj: TObject;
// field: TdormFieldMapping;
// v: TValue;
// begin
// Result := False;
// obj := AObject;
// for field in AFieldsMapping do
// begin
// if CompareText(field.field_type, 'string') = 0 then
// v := AReader.Value[AReader.GetOrdinal(field.field)].AsString
// else if CompareText(field.field_type, 'integer') = 0 then
// v := AReader.Value[AReader.GetOrdinal(field.field)].AsInt32
// else if CompareText(field.field_type, 'date') = 0 then
// begin
// v := AReader.Value[AReader.GetOrdinal(field.field)].AsDate
//
// end
// else
// raise Exception.Create('Unknown field type for ' + field.field);
// TdormUtils.SetField(obj, field.name, v);
// end;
// Result := true;
// end;

function TSqlite3PersistStrategy.RawExecute(SQL: string): Int64;
begin
  GetLogger.Warning('RAW EXECUTE: ' + SQL);
  Result := 0;
  DB.ExecSQL(SQL); // sqlite3 do not return affected rows?
end;

function TSqlite3PersistStrategy.CreateObjectFromSqliteTable(ARttiType: TRttiType;
  AReader: TSqliteTable; AMappingTable: TMappingTable): TObject;
var
  obj: TObject;
begin
  obj := TdormUtils.CreateObject(ARttiType);
  LoadObjectFromSqliteTable(obj, ARttiType, AReader, AMappingTable.Fields);
  Result := obj;
end;

class procedure TSqlite3PersistStrategy.register;
begin
  //
end;

procedure TSqlite3PersistStrategy.Rollback;
begin
  if not DB.InTransaction then
    raise Exception.Create('Transaction is not active');
  DB.Rollback('trans');
end;

procedure TSqlite3PersistStrategy.SetSqlite3ParameterValue(ADB: TSQLiteDatabase; aFieldType: string;
  aParameterName: String; aValue: TValue);
var
  sourceStream: TStream;
  str: TMemoryStream;
begin
  if CompareText(aFieldType, 'string') = 0 then
  begin
    ADB.AddParamText(aParameterName, aValue.AsString);
    GetLogger.Debug(aParameterName + ' = ' + aValue.AsString);
  end
  else if (CompareText(aFieldType, 'integer') = 0) or (CompareText(aFieldType, 'int64') = 0) then
  begin
    ADB.AddParamInt(aParameterName, aValue.AsInteger);
    GetLogger.Debug(aParameterName + ' = ' + IntToStr(aValue.AsInteger));
  end
  else if CompareText(aFieldType, 'decimal') = 0 then
  begin
    ADB.AddParamFloat(aParameterName, aValue.AsExtended);
    GetLogger.Debug(aParameterName + ' = ' + FloatToStr(aValue.AsExtended));
  end
  else if CompareText(aFieldType, 'boolean') = 0 then
  begin
    if aValue.AsBoolean then
    begin
      ADB.AddParamInt(aParameterName, 1);
      GetLogger.Debug(aParameterName + ' = 1');
    end
    else
    begin
      ADB.AddParamInt(aParameterName, 0);
      GetLogger.Debug(aParameterName + ' = 0');
    end;
  end
  else if CompareText(aFieldType, 'date') = 0 then
  begin
    ADB.AddParamText(aParameterName, ISODateToString(aValue.AsExtended));
    // EscapeDate(aValue.AsExtended));
    GetLogger.Debug(aParameterName + ' = ' +
      EscapeDate(DateTimeToTimeStamp(aValue.AsExtended).Date));
  end
  else if CompareText(aFieldType, 'datetime') = 0 then
  begin
    if aValue.AsExtended = 0 then begin
      ADB.AddParamNull(aParameterName);
      GetLogger.Debug(aParameterName + ' = NULL');
    end else begin
      if FStandardDateFormat = '' then begin
        ADB.AddParamText(aParameterName, ISODateTimeToString(aValue.AsExtended));
      end else begin
        ADB.AddParamText(aParameterName, FormatDateTime(FStandardDateFormat, aValue.AsExtended, TFormatSettings.Create));
      end;
      // EscapeDateTime(FloatToDateTime(aValue.AsExtended)));
      GetLogger.Debug(aParameterName + ' = ' + EscapeDateTime(FloatToDateTime(aValue.AsExtended)));
    end;
  end
  else if CompareText(aFieldType, 'time') = 0 then
  begin
    ADB.AddParamText(aParameterName, ISOTimeToString(Frac(FloatToDateTime(aValue.AsExtended))));
    // EscapeDateTime(Frac(FloatToDateTime(aValue.AsExtended))));
    GetLogger.Debug(aParameterName + ' = ' +
      EscapeDateTime(Frac(FloatToDateTime(aValue.AsExtended))));
  end
  else if CompareText(aFieldType, 'blob') = 0 then
  begin
    sourceStream := TStream(aValue.AsObject);
    if sourceStream = nil then
    begin
      ADB.AddParamNull(aParameterName);
      GetLogger.Debug(aParameterName + ' = NULL');
    end
    else
    begin
      str := TMemoryStream.Create;
      try
        sourceStream.Position := 0;
        str.CopyFrom(sourceStream, sourceStream.Size);
        str.Position := 0;
        ADB.AddParamBlobPtr(aParameterName, str.Memory, str.Size);
        GetLogger.Debug(aParameterName + ' = <' + IntToStr(str.Size) + ' bytes>');
      finally
        str.Free;
      end;
    end;
  end;
end;

// procedure TSqlite3PersistStrategy.SetDBXValue(aFieldType: string;
// aDBXValue: TDBXWritableValue; aValue: TValue);
//
// var
// str: TBytesStream;
// sourceStream: TStream;
// begin
// if CompareText(aFieldType, 'string') = 0 then
// begin
// aDBXValue.AsString := aValue.AsString;
// end
// else if CompareText(aFieldType, 'integer') = 0 then
// begin
// aDBXValue.AsBcd := IntegerToBcd(aValue.AsInt64);
// end
// else if CompareText(aFieldType, 'date') = 0 then
// begin
// aDBXValue.AsDate := DateTimeToTimeStamp(aValue.AsExtended).Date;
// end
// else if CompareText(aFieldType, 'datetime') = 0 then
// begin
// aDBXValue.AsDateTime := FloatToDateTime(aValue.AsExtended);
// end
//
// else if CompareText(aFieldType, 'decimal') = 0 then
// begin
// aDBXValue.AsDouble := aValue.AsExtended;
// end
//
//
// else if CompareText(aFieldType, 'blob') = 0 then
// begin
// sourceStream := TStream(aValue.AsObject);
// if sourceStream = nil then
// aDBXValue.SetNull
// else
// begin
// str := TBytesStream.Create;
// try
// sourceStream.Position := 0;
// str.CopyFrom(sourceStream, sourceStream.Size);
// str.Position := 0;
// aDBXValue.SetStream(str, true);
// aDBXValue.ValueType.ValueTypeFlags :=
// aDBXValue.ValueType.ValueTypeFlags or TDBXValueTypeFlags.ExtendedType;
// except
// str.Free;
// raise;
// end;
// end;
// end
// else if CompareText(aFieldType, 'decimal') = 0 then
// begin
// aDBXValue.AsDouble := aValue.AsExtended;
// end
// else if CompareText(aFieldType, 'boolean') = 0 then
// begin
// if aValue.AsBoolean then
// aDBXValue.AsInt16 := 1
// else
// aDBXValue.AsInt16 := 0;
// end
// else
// raise Exception.Create('Unsupported type ' + IntToStr(ord(aValue.Kind)));
//
// end;

procedure TSqlite3PersistStrategy.SetLogger(ALogger: IdormLogger);
begin
  FLogger := ALogger;
end;

procedure TSqlite3PersistStrategy.StartTransaction;
begin
  if DB.InTransaction then
    raise Exception.Create('Transaction already active');
  DB.Start('trans');
end;

initialization

TSqlite3PersistStrategy.register;

finalization

end.

