unit dorm.adapter.FireDAC.BaseAdapter;


////////////////////////
//Contributors
//- Marco Mottadelli
//- Mauro Catellani
////////////////////////

interface

uses
  dorm.Commons,
  dorm.Mappings,
  classes,
  SysUtils,
  DB,
  Rtti,
  dorm,
  superobject,
  TypInfo,
  FMTBcd,
  dorm.Filters,
  Generics.Collections,
  dorm.Collections,
  dorm.adapter.FireDAC.Facade,
  dorm.adapter.Base,
  dorm.Mappings.Strategies,
  FireDAC.Comp.Client,
  FireDAC.Stan.Param;

type
  TFireDACBaseAdapter = class(TBaseAdapter, IdormPersistStrategy)
  private
    function GetFireDACReaderFor(ARttiType: TRttiType; AMappingTable: TMappingTable; const Value: TValue;
      AMappingRelationField: TMappingField = nil): TFDQuery;
    function GetSqlFieldsForUpdate(AMappingTable: TMappingTable; AObject: TObject; _ParamFields : TStringList): string;
    procedure SetNullParameterValue(AParam: TFDParam);
  protected
    FFormatSettings: TFormatSettings;
    FD: TFireDACFacade;
    FLogger: IdormLogger;
    procedure InitFormatSettings;
    function CreateFireDACFacade(Conf: ISuperObject): TFireDACFacade; virtual; abstract;
    function CreateObjectFromFireDACQuery(ARttiType: TRttiType; AReader: TFDQuery;
      AMappingTable: TMappingTable): TObject;
    procedure LoadObjectFromFireDACReader(AObject: TObject; ARttiType: TRttiType; AReader: TFDQuery;
  AFieldsMapping: TMappingFieldList; const _FieldQualifier : string = '');
    function GetLogger: IdormLogger;
    procedure SetFireDACParameterValue(AField: TMappingField; AStatement: TFDQuery; AValue: TValue; AIsNullable: boolean = False);
    function EscapeDateTime(const Value: TDate; AWithMillisSeconds : boolean = false): string; override;
  public
    // Start Method Interface IdormPersistStrategy
    function GetLastInsertOID: TValue; overload;
    function GetLastInsertOID(_query : TFDQuery): TValue; overload;
    function Insert(ARttiType: TRttiType; AObject: TObject; AMappingTable: TMappingTable): TValue;
    function Update(ARttiType: TRttiType; AObject: TObject; AMappingTable: TMappingTable;
      ACurrentVersion: Int64): Int64;
    function Delete(ARttiType: TRttiType; AObject: TObject; AMappingTable: TMappingTable;
      ACurrentVersion: Int64): Int64;
    procedure DeleteAll(AMappingTable: TMappingTable);
    function Count(AMappingTable: TMappingTable): Int64;
    function Load(ARttiType: TRttiType; AMappingTable: TMappingTable; AMappingRelationField: TMappingField;
      const Value: TValue; AObject: TObject; const DontRaiseExceptionOnUnexpectedMultiRowResult: Boolean): boolean; overload;
    function Load(ARttiType: TRttiType; AMappingTable: TMappingTable; const Value: TValue; AObject: TObject)
      : boolean; overload;
    procedure LoadList(AList: TObject; ARttiType: TRttiType; AMappingTable: TMappingTable;
      ACriteria: ICriteria); overload;
    procedure ConfigureStrategy(ConfigurationInfo: ISuperObject; const UnicodeDB: Boolean); virtual;
    procedure InitStrategy;
    procedure StartTransaction;
    procedure Commit;
    procedure Rollback;
    function InTransaction: boolean;
    procedure SetLogger(ALogger: IdormLogger);
    function RawExecute(SQL: string): Int64;
    function ExecuteAndGetFirst(SQL: string): Int64;
    function GetDatabaseBuilder(AEntities: TList<string>; AMappings: ICacheMappingStrategy): IDataBaseBuilder;
    function ExecuteCommand(ACommand: IdormCommand): Int64;
    procedure ExecStoredProcedure(const AProcName: String; _InputParams, _OutputParams: TStringList);
    function GetConnection : TCustomConnection;
    function GetFieldTypeAndSize(const _Table, _Field: string; out _Type: TFieldType; out _Size: integer): Boolean;
    function GetDBName: string;
    function GetEngine: string; virtual; abstract;
    // End Method Interface IdormPersistStrategy
    destructor Destroy; override;
    class procedure register;
  end;

implementation

uses
  dorm.Utils, System.Types, System.StrUtils, System.Generics.Defaults;

procedure TFireDACBaseAdapter.InitFormatSettings;
begin
  FFormatSettings.LongDateFormat := 'YYYY-MM-DD';
  FFormatSettings.ShortDateFormat := 'YYYY-MM-DD';
  FFormatSettings.LongTimeFormat := 'HH:NN:SS';
  FFormatSettings.ShortTimeFormat := 'HH:NN:SS';
  FFormatSettings.DateSeparator := '-';
  FFormatSettings.TimeSeparator := ':';
end;

procedure TFireDACBaseAdapter.InitStrategy;
begin
end;

function TFireDACBaseAdapter.Update(ARttiType: TRttiType; AObject: TObject; AMappingTable: TMappingTable;
  ACurrentVersion: Int64): Int64;
var
  field: TMappingField;
  SQL: string;
  Query: TFDQuery;
  I, pk_idx: Integer;
  v: TValue;
  sql_fields_names: string;
  pk_field, pk_val: string;
  isNullable: boolean;
  PKMappingIndexes: TIntegerDynArray;
  ParamFields : TStringList;
  ts : TDateTime;
begin
  ParamFields := TStringList.Create;
  try
    sql_fields_names := GetSqlFieldsForUpdate(AMappingTable, AObject, ParamFields);
    if sql_fields_names <> '' then begin
      // pk_field aufblasen fuer where
      PKMappingIndexes := GetPKMappingIndexes(AMappingTable.Fields);
      pk_field := AMappingTable.Fields[PKMappingIndexes[0]].FieldName;
      SQL := Format('UPDATE %S SET %S WHERE [%S] = :' + pk_field, [AMappingTable.TableName, sql_fields_names, pk_field]);
      for I := 1 to Length(PKMappingIndexes) - 1 do begin
        pk_field := AMappingTable.Fields[PKMappingIndexes[I]].FieldName;
        if AMappingTable.Fields[PKMappingIndexes[I]].FieldType = 'datetime' then begin
          pk_val := EscapeDateTime(ARttiType.GetProperty(AMappingTable.Fields[I].name).GetValue(AObject).AsExtended, true)
        end else begin
          pk_val := ARttiType.GetProperty(AMappingTable.Fields[I].name).GetValue(AObject).AsString;
        end;
        SQL := SQL + Format(' AND [%S] = ''' +  pk_val + '''', [pk_field]);
      end;
      if ACurrentVersion >= 0 then
      begin
        SQL := SQL + ' AND OBJVERSION = ' + IntToStr(ACurrentVersion);
      end;
      GetLogger.Debug(AMappingTable.Fields[PKMappingIndexes[0]].FieldName);
      GetLogger.Debug('NEW QUERY: ' + SQL);
      Query := FD.NewQuery;
      Query.SQL.Text := SQL;
      try
        for field in AMappingTable.Fields do begin
          // manage nullable fields
          isNullable := TdormUtils.HasAttribute<Nullable>(field.RTTICache.RTTIProp);
          if ParamFields.IndexOf(field.FieldName) >= 0 then begin
            v := TdormUtils.GetField(AObject, field.name);
            SetFireDACParameterValue(field, Query, v, isNullable);
          end;
        end;
        // Retrieve pk index
        pk_idx := GetPKMappingIndex(AMappingTable.Fields);
        // Retrieve pk value
        v := ARttiType.GetProperty(AMappingTable.Fields[pk_idx].name).GetValue(AObject);
        // Set pk parameter value
        SetFireDACParameterValue(AMappingTable.Fields[pk_idx], Query, v);

        ts := Now;
        GetLogger.Debug('EXECUTING PREPARED: ' + SQL);
        try
          Result := FD.Execute(Query);
          GetLogger.LogCall(Query.SQL.Text, ts);
        except
          on e : Exception do begin
            GetLogger.Error(e.Message, Query.SQL.Text);
            raise;
          end;
        end;
      finally
        Query.Free;
      end;
    end else begin
      GetLogger.Debug('No update required, record unchanged');
    end;
  finally
    ParamFields.Free;
  end;
end;

procedure TFireDACBaseAdapter.Commit;
begin
  FD.CommitTransaction;
end;

procedure TFireDACBaseAdapter.ConfigureStrategy(ConfigurationInfo: ISuperObject; const UnicodeDB: Boolean);
begin
  FD := CreateFireDACFacade(ConfigurationInfo);
  FD.IsUnicodeDB := UnicodeDB;
  // TODO: We must implement here keys generator part for other database
  // In SqlServer this part isn't necessary because sequence is missing
  inherited;
end;

function TFireDACBaseAdapter.Count(AMappingTable: TMappingTable): Int64;
var
  Qry: TFDQuery;
  SQL: string;
begin
  Result := -1;
  SQL := 'SELECT COUNT(*) AS CNT FROM ' + AMappingTable.TableName;
  GetLogger.Debug('PREPARING: ' + SQL);
  Qry := FD.NewQuery;
  try
    Qry.SQL.Text := SQL;
    Qry.Open;
    if not Qry.Eof then
      Result := Qry.FieldByName('CNT').AsLargeInt;
    Qry.Close;
  finally
    Qry.Free;
  end;
end;

function TFireDACBaseAdapter.Delete(ARttiType: TRttiType; AObject: TObject; AMappingTable: TMappingTable;
  ACurrentVersion: Int64): Int64;
var
  pk_idx: Integer;
  pk_value: TValue;
  pk_attribute_name, pk_field_name, SQL: string;
  Qry: TFDQuery;
begin
  pk_idx := GetPKMappingIndex(AMappingTable.Fields);
  if pk_idx = -1 then
    raise Exception.Create('Invalid primary key for table ' + AMappingTable.TableName);
  pk_attribute_name := AMappingTable.Fields[pk_idx].name;
  pk_field_name := AMappingTable.Fields[pk_idx].FieldName;
  pk_value := ARttiType.GetProperty(pk_attribute_name).GetValue(AObject);
  SQL := 'DELETE FROM ' + AMappingTable.TableName + ' WHERE [' + pk_field_name + '] = :' + pk_field_name;
  if ACurrentVersion >= 0 then
    SQL := SQL + ' AND OBJVERSION = ' + IntToStr(ACurrentVersion);

  GetLogger.Debug('NEW QUERY: ' + SQL);
  Qry := FD.NewQuery;
  try
    Qry.SQL.Text := SQL;
    if pk_value.IsOrdinal then begin
      Qry.Params[0].DataType := ftLargeint;
      Qry.Params[0].AsLargeInt := pk_value.AsInt64;
    end else begin
      Qry.Params[0].DataType := ftString;
      Qry.Params[0].AsString := pk_value.AsString;
    end;
    GetLogger.Debug('EXECUTING QUERY: ' + SQL);
    Qry.ExecSQL;
    Result := Qry.RowsAffected;
  finally
    Qry.Free;
  end;
end;

procedure TFireDACBaseAdapter.DeleteAll(AMappingTable: TMappingTable);
var
  SQL: string;
  ts : TDateTime;
begin
  SQL := 'DELETE FROM ' + AMappingTable.TableName;
  GetLogger.Debug('EXECUTING :' + SQL);
  ts := Now;
  try
    FD.Execute(SQL);
    GetLogger.LogCall(SQL, ts);
  except
    on e : Exception do begin
      GetLogger.Error(e.Message, SQL);
      raise;
    end;
  end;
end;

destructor TFireDACBaseAdapter.Destroy;
begin
  FD.Free;
  inherited;
end;

function TFireDACBaseAdapter.EscapeDateTime(const Value: TDate; AWithMillisSeconds : boolean = false): string;
begin
  Result := FormatDateTime('YYYY-MM-DD&HH:NN:SS' + IfThen(AWithMillisSeconds, '.ZZZ'), Value);
  Result := StringReplace(Result, '&', 'T', [rfReplaceAll]);
end;

procedure TFireDACBaseAdapter.ExecStoredProcedure(const AProcName: String; _InputParams, _OutputParams: TStringList);
begin
  FD.ExecStoredProcedure(AProcName, _InputParams, _OutputParams);
end;

function TFireDACBaseAdapter.ExecuteAndGetFirst(SQL: string): Int64;
var
  Qry: TFDQuery;
  ts : TDateTime;
begin
  GetLogger.EnterLevel('ExecuteAndGetFirst');
  GetLogger.Info('PREPARING: ' + SQL);
  Qry := FD.NewQuery;
  try
    GetLogger.Info('EXECUTING: ' + SQL);
    Qry.SQL.Text := SQL;
    ts := Now;
    try
      Qry.Open;
      if not Qry.Eof then
        Result := Int64(Qry.Fields[0].AsLargeInt)
      else
        raise EdormException.Create('ExecuteAndGetFirst returns 0 rows');
      Qry.Close;
      GetLogger.LogCall(SQL, ts);
    except
      on e : Exception do begin
        GetLogger.Error(e.Message, SQL);
        raise;
      end;
    end;
  finally
    GetLogger.ExitLevel('ExecuteAndGetFirst');
    Qry.Free;
  end;
end;

function TFireDACBaseAdapter.ExecuteCommand(ACommand: IdormCommand): Int64;
var
  SQL: string;
  Qry: TFDQuery;
  ts : TDateTime;
begin
  SQL := ACommand.GetSQL;
  GetLogger.Debug('EXECUTING: ' + SQL);
  Qry := FD.NewQuery;
  ts := Now;
  try
    // if reader.Params.ParamCount <> 0 then
    // raise EdormException.Create('Parameters not replaced');
    Qry.SQL.Text := SQL;
    try
      Qry.Execute;
      GetLogger.LogCall(SQL, ts);
    except
      on e : Exception do begin
        GetLogger.Error(e.Message, SQL);
        raise;
      end;
    end;
    Result := Qry.RowsAffected;
  finally
    Qry.Free;
  end;
end;

function TFireDACBaseAdapter.GetConnection: TCustomConnection;
begin
  Result := FD.GetConnection;
end;

function TFireDACBaseAdapter.GetDatabaseBuilder(AEntities: TList<string>; AMappings: ICacheMappingStrategy)
  : IDataBaseBuilder;
begin
  AEntities.Free; // just to hide the memory leak
  raise Exception.Create('Not implemented for ' + self.ClassName);
end;

function TFireDACBaseAdapter.GetDBName: string;
begin
  Result := FD.GetConnection.Params.Database;
end;

function TFireDACBaseAdapter.GetLastInsertOID: TValue;
var
  Qry: TFDQuery;
begin
  Qry := FD.NewQuery;
  try
    Result := GetLastInsertOID(Qry);
  finally
    Qry.Free;
  end;
end;

function TFireDACBaseAdapter.GetLogger: IdormLogger;
begin
  Result := FLogger;
end;

function TFireDACBaseAdapter.Insert(ARttiType: TRttiType; AObject: TObject; AMappingTable: TMappingTable): TValue;
var
  field: TMappingField;
  sql_fields_names, sql_fields_values, SQL: String;
  Query: TFDQuery;
  pk_idx: Integer;
  v, pk_value: TValue;
  isTransient: boolean;
  isNullable: boolean;
  FuncVal : ValueFromFunction;
  ParamFields : TStringList;
  ts : TDateTime;
begin
  ParamFields := TStringList.Create;
  try
    sql_fields_names := '';
    sql_fields_values := '';

    for field in AMappingTable.Fields do
    begin
      // manage transients fields
      isTransient := TdormUtils.HasAttribute<Transient>(field.RTTICache.RTTIProp);

      if (not field.IsPK or field.IsFK) and (not isTransient) then
      begin
        v := TdormUtils.GetField(AObject, field.RTTICache);
        // Compose Fields Names and Values
        sql_fields_names := sql_fields_names + ',[' + field.FieldName + ']';

        FuncVal := TdormUtils.GetAttribute<ValueFromFunction>(field.RTTICache.RTTIProp);
        if Assigned(FuncVal) then begin
          if FuncVal.TriggerVal.IsEmpty or TdormUtils.EqualValues(FuncVal.TriggerVal, v) then begin
            sql_fields_values := sql_fields_values + ',dbo.' + FuncVal.FuncName;
            continue;
          end;
        end;
        sql_fields_values := sql_fields_values + ',:' + field.FieldName;
        ParamFields.Add(field.FieldName);
      end;
    end;
    System.Delete(sql_fields_names, 1, 1);
    System.Delete(sql_fields_values, 1, 1);
    SQL := Format('INSERT INTO %s (%s) VALUES (%s)', [AMappingTable.TableName, sql_fields_names, sql_fields_values]);
    GetLogger.Debug('PREPARING :' + SQL);

    Query := FD.NewQuery;
    Query.SQL.Text := SQL;
    try
      for field in AMappingTable.Fields do begin
        v := TdormUtils.GetField(AObject, field.RTTICache);
        // manage nullable fields
        isNullable := TdormUtils.HasAttribute<Nullable>(field.RTTICache.RTTIProp);

        if ParamFields.IndexOf(field.FieldName) >= 0 then begin
          v := TdormUtils.GetField(AObject, field.RTTICache);
          SetFireDACParameterValue(field, Query, v, isNullable);
        end;
      end;
      ts := Now;
      GetLogger.Debug('EXECUTING PREPARED :' + string(SQL));
      pk_idx := GetPKMappingIndex(AMappingTable.Fields);
      try
        if pk_idx <> -1 then begin
          pk_value := GetLastInsertOID(Query);
          TdormUtils.SetField(AObject, AMappingTable.Fields[pk_idx].RTTICache, pk_value);
        end else begin
          FD.Execute(Query);
          pk_value := TValue.Empty;
        end;
        Result := pk_value;
        GetLogger.LogCall(Query.SQL.Text, ts);
      except
        on e : Exception do begin
          GetLogger.Error(e.Message, Query.SQL.Text);
          raise;
        end;
      end;
    finally
      Query.Free;
    end;
  finally
    ParamFields.Free;
  end;
end;

function TFireDACBaseAdapter.InTransaction: boolean;
var
  tr: TFDTransaction;
begin
  tr := FD.GetCurrentTransaction;
  Result := assigned(tr);
  if Result then
    Result := tr.Active;
end;

function TFireDACBaseAdapter.GetLastInsertOID(_query : TFDQuery): TValue;
var
  SQL: String;
  field : TField;
begin
  if _query.SQL.Count = 0 then begin
    SQL := 'SELECT @@IDENTITY AS LAST_IDENTITY';
  end else begin
    SQL := 'SELECT SCOPE_IDENTITY() AS LAST_IDENTITY';
  end;
  GetLogger.Debug('PREPARING: ' + SQL);
  GetLogger.Debug('EXECUTING PREPARED: ' + SQL);
  _query.SQL.Add(';' + SQL);
  _query.Open;
  _Query.First;
  if not _query.Eof then
  begin
    field := _query.FieldByName('LAST_IDENTITY');
    if (not field.IsNull) then
    begin
      Result := field.AsLargeInt;
    end
    else
      Result := TValue.Empty;
  end;
  _query.Close;
end;

function TFireDACBaseAdapter.GetSqlFieldsForUpdate(AMappingTable: TMappingTable; AObject: TObject; _ParamFields : TStringList): string;
var
  field: TMappingField;
  isTransient: boolean;
  FuncVal : ValueFromFunction;
  v, vbak: TValue;
  prop : TRttiProperty;
  BackupObj : TObject;
begin
  _ParamFields.Clear;
  _ParamFields.Sorted := True;
  Result := '';
  prop := TdormUtils.GetPropertyDef(AObject, 'Backup');
  if assigned(prop) then begin
    BackupObj := TdormUtils.GetProperty(AObject, 'Backup').AsObject;
  end else begin
    BackupObj := nil;
  end;

  for field in AMappingTable.Fields do
  begin
    // manage transients fields
    isTransient := TdormUtils.HasAttribute<Transient>(field.RTTICache.RTTIProp);

    if (not field.IsPK) and (not isTransient) then
    begin
      v := TdormUtils.GetField(AObject, field.name);
      FuncVal := TdormUtils.GetAttribute<ValueFromFunction>(field.RTTICache.RTTIProp);
      if Assigned(FuncVal) then begin
        if FuncVal.TriggerVal.IsEmpty or TdormUtils.EqualValues(FuncVal.TriggerVal, v) then begin
          Result := Result + ',[' + field.FieldName + '] = dbo.' + FuncVal.FuncName;
          continue;
        end;
      end;
      if assigned(BackupObj) then begin
        vbak := TdormUtils.GetField(BackupObj, field.name);
        if v.AsVariant = vbak.AsVariant then begin
          continue;
        end;
      end;
      Result := Result + ',[' + field.FieldName + '] = :' + field.FieldName;
      _ParamFields.Add(field.FieldName)
    end;
  end;
  System.Delete(Result, 1, 1);
end;

function TFireDACBaseAdapter.GetFieldTypeAndSize(const _Table, _Field: string;
  out _Type: TFieldType; out _Size: integer): Boolean;
var Query: TFDQuery;
begin
  Query := FD.NewQuery;
  try
    Query.Open('select ' + _Field + ' from ' + _Table + ' where 1 = 0');
    Query.Open;
    _Type := Query.Fields[0].DataType;
    _Size := Query.Fields[0].Size;
    Query.Close;
    Result := True;
  except
    _Type := ftUnknown;
    _Size := 0;
    Result:= False;
  end;
  Query.Free;
end;

function TFireDACBaseAdapter.GetFireDACReaderFor(ARttiType: TRttiType; AMappingTable: TMappingTable;
  const Value: TValue; AMappingRelationField: TMappingField): TFDQuery;
var
  pk_idx: Integer;
  pk_field_name, SQL, ValStr: string;
begin
  if Value.IsOrdinal then begin
    ValStr := Value.AsInt64.ToString;
  end else begin
    ValStr := QuotedStr(Value.AsString);
  end;

  if AMappingRelationField = nil then
  begin
    pk_idx := GetPKMappingIndex(AMappingTable.Fields);
    if pk_idx = -1 then
      raise Exception.Create('Invalid primary key for table ' + AMappingTable.TableName);
    pk_field_name := AMappingTable.Fields[pk_idx].FieldName;
    SQL := 'SELECT ' + GetSelectFieldsList(AMappingTable.Fields, true) + ' FROM ' + AMappingTable.TableName + ' WHERE ['
      + pk_field_name + '] = ' + ValStr;
  end
  else
  begin
    pk_idx := GetPKMappingIndex(AMappingTable.Fields);
    if pk_idx = -1 then
      raise Exception.Create('Invalid primary key for table ' + AMappingTable.TableName);
    pk_field_name := AMappingTable.Fields[pk_idx].FieldName;
    SQL := 'SELECT ' + GetSelectFieldsList(AMappingTable.Fields, true) + ' FROM ' + AMappingTable.TableName + ' WHERE ['
      + AMappingRelationField.FieldName + '] = ' + ValStr;
  end;
  GetLogger.Debug('PREPARING: ' + SQL);
  Result := FD.NewQuery;
  Result.SQL.Text := SQL;
end;

function TFireDACBaseAdapter.Load(ARttiType: TRttiType; AMappingTable: TMappingTable;
  AMappingRelationField: TMappingField; const Value: TValue; AObject: TObject; const DontRaiseExceptionOnUnexpectedMultiRowResult: Boolean): boolean;
var
  reader: TFDQuery;
  ts: TDateTime;
begin
  ts := now;
  reader := GetFireDACReaderFor(ARttiType, AMappingTable, Value, AMappingRelationField);
  try
    try
      reader.Open();
      GetLogger.LogCall(reader.SQL.Text, ts);
      Result := not reader.Eof;
      if Result then
        LoadObjectFromFireDACReader(AObject, ARttiType, reader, AMappingTable.Fields);
      reader.Next;
      if not reader.Eof and not DontRaiseExceptionOnUnexpectedMultiRowResult then
        // there is some problem.... here I should have only one record
        raise EdormException.Create('Singleton select returns more than 1 record');
    except
      on e : Exception do begin
        GetLogger.Error(e.Message, reader.SQL.Text);
        raise;
      end;
    end;
  finally
    reader.Free;
  end;
end;

procedure TFireDACBaseAdapter.LoadList(AList: TObject; ARttiType: TRttiType; AMappingTable: TMappingTable;
  ACriteria: ICriteria);
var
  SQL: string;
  reader: TFDQuery;
  CustomCriteria: ICustomCriteria;
  ts :  TDatetime;
begin
  if assigned(ACriteria) and TInterfacedObject(ACriteria).GetInterface(ICustomCriteria, CustomCriteria) then
    SQL := CustomCriteria.GetSQL
  else
    SQL := self.GetSelectSQL(ACriteria, AMappingTable);
  GetLogger.Debug('EXECUTING: ' + SQL);
  reader := FD.NewQuery;
  try
    // if reader.Params.ParamCount <> 0 then
    // raise EdormException.Create('Parameters not replaced');
    ts := Now;
    reader.SQL.Text := SQL;
    try
      reader.Open();
      GetLogger.LogCall(SQL, ts);
      while not reader.Eof do begin
        TdormUtils.MethodCall(AList, 'Add', [CreateObjectFromFireDACQuery(ARttiType, reader, AMappingTable)]);
        reader.Next;
      end;
      reader.Close;
    except
      on e : Exception do begin
        GetLogger.Error(e.Message, SQL);
        raise;
      end;
    end;
  finally
    reader.Free;
  end;
end;

procedure TFireDACBaseAdapter.LoadObjectFromFireDACReader(AObject: TObject; ARttiType: TRttiType; AReader: TFDQuery;
  AFieldsMapping: TMappingFieldList; const _FieldQualifier : string = '');
var
  field: TMappingField;
  SortedFields : TMappingFieldList;
  v: TValue;
  S: string;
  sourceStream: TStringStream;
  f: TField;
  Take : boolean;
  i: Integer;
begin
  SortedFields := TMappingFieldList.Create;
  try
    try
      SortedFields.OwnsObjects := False;
      SortedFields.AddRange(AFieldsMapping);
      SortedFields.Sort;
      Take := _FieldQualifier = '';
      for i := 0 to AReader.Fields.Count - 1 do begin
        if AReader.Fields[i].FieldName.StartsWith('#') then begin
          if Take then begin
            Break;
          end;
          Take := AReader.Fields[i].FieldName.Substring(1).Equals(_FieldQualifier);
          Continue;
        end;
        if Take then begin
          field := SortedFields.BinSearchByFieldName(AReader.Fields[i].Origin);
          if assigned(field) then begin
            if CompareText(field.FieldType, 'string') = 0 then
            begin
              v := AReader.Fields[i].AsString;
              S := field.FieldName + ' as string';
            end
            else if CompareText(field.FieldType, 'integer') = 0 then
            begin
              v := AReader.Fields[i].AsInteger;
              S := field.FieldName + ' as integer';
            end
            else if CompareText(field.FieldType, 'int64') = 0 then
            begin
              v := AReader.Fields[i].AsLargeInt;
              S := field.FieldName + ' as int64';
            end
            else if CompareText(field.FieldType, 'date') = 0 then
            begin
              v := AReader.Fields[i].AsDateTime;
              S := field.FieldName + ' as date';
            end
            else if CompareText(field.FieldType, 'blob') = 0 then
            begin
              S := field.FieldName + ' as blob';
              sourceStream := nil;
              if not AReader.Fields[i].IsNull then
              begin
                sourceStream := TStringStream.Create(AReader.Fields[i].AsBytes);
              end;
              if assigned(sourceStream) then
              begin
                sourceStream.Position := 0;
                v := sourceStream;
              end
              else
                v := nil;
            end
            else if CompareText(field.FieldType, 'decimal') = 0 then
            begin
              v := AReader.Fields[i].AsFloat;
              S := field.FieldName + ' as decimal';
            end
            else if CompareText(field.FieldType, 'boolean') = 0 then
            begin
              f := AReader.Fields[i];
              if f.DataType = ftBoolean then
              begin
                v := AReader.Fields[i].AsBoolean
              end
              else
              begin
                v := AReader.Fields[i].AsInteger <> 0;
              end;
              S := field.FieldName + ' as boolean';
            end
            else if CompareText(field.FieldType, 'datetime') = 0 then
            begin
              v := AReader.Fields[i].AsDateTime;
              S := field.FieldName + ' as datetime';
            end
            else if CompareText(field.FieldType, 'time') = 0 then
            begin
              v := AReader.Fields[i].AsDateTime;
              S := field.FieldName + ' as time';
            end
            else if CompareText(field.FieldType, 'float') = 0 then
            begin
              v := AReader.Fields[i].AsFloat;
              S := field.FieldName + ' as float';
            end
            else
              raise Exception.Create('Unknown field type for ' + field.FieldName);
            try
              TdormUtils.SetField(AObject, field.name, v);
            except
              on E: Exception do
              begin
                raise EdormException.Create(E.Message + sLineBreak + '. Probably cannot write ' + ARttiType.ToString
                  + '.' + S);
              end;
            end;
          end;
        end;
      end;
    except
      on E: Exception do
      begin
        raise;
      end;
    end;
  finally
    SortedFields.DisposeOf;
  end;
end;

function TFireDACBaseAdapter.RawExecute(SQL: string): Int64;
var ts : TDateTime;
begin
  GetLogger.Warning('RAW EXECUTE: ' + SQL);
  ts := Now;
  try
    Result := FD.Execute(SQL);
    GetLogger.LogCall(SQL, ts);
  except
    on e : Exception do begin
      GetLogger.Error(e.Message, SQL);
      raise;
    end;
  end;
end;

function TFireDACBaseAdapter.CreateObjectFromFireDACQuery(ARttiType: TRttiType; AReader: TFDQuery;
  AMappingTable: TMappingTable): TObject;
var
  obj, subobj: TObject;
  field: TMappingField;
  v: TValue;
  S: string;
  targetStream: TMemoryStream;
  f: TField;
  i: Integer;
  ctx : TRttiContext;
begin
  ctx:= TRttiContext.Create;
  try
    try
      obj := TdormUtils.CreateObject(ARttiType);
      for field in AMappingTable.Fields do
      begin
        if CompareText(field.FieldType, 'string') = 0 then
        begin
          v := AReader.FieldByName(field.FieldName).AsString;
          S := field.FieldName + ' as string';
        end
        else if CompareText(field.FieldType, 'integer') = 0 then
        begin
          v := AReader.FieldByName(field.FieldName).AsInteger;
          S := field.FieldName + ' as integer';
        end
        else if CompareText(field.FieldType, 'int64') = 0 then
        begin
          v := AReader.FieldByName(field.FieldName).AsLargeInt;
          S := field.FieldName + ' as int64';
        end
        else if CompareText(field.FieldType, 'date') = 0 then
        begin
          v := trunc(AReader.FieldByName(field.FieldName).AsDateTime);
          S := field.FieldName + ' as date';
        end
        else if CompareText(field.FieldType, 'blob') = 0 then
        begin
          targetStream := nil;
          if not AReader.FieldByName(field.FieldName).IsNull then
          begin
            targetStream := TStringStream.Create(AReader.FieldByName(field.FieldName).AsBytes);
            // targetStream := TMemoryStream(AReader.CreateBlobStream(AReader.FieldByName(field.FieldName), bmRead));
            targetStream.Position := 0;
          end;
          S := field.FieldName + ' as blob';
          v := targetStream;
        end
        else if CompareText(field.FieldType, 'decimal') = 0 then
        begin
          v := AReader.FieldByName(field.FieldName).AsFloat;
          S := field.FieldName + ' as decimal';
        end
        else if CompareText(field.FieldType, 'float') = 0 then
        begin
          v := AReader.FieldByName(field.FieldName).AsFloat;
          S := field.FieldName + ' as float';
        end
        else if CompareText(field.FieldType, 'boolean') = 0 then
        begin
          f := AReader.FieldByName(field.FieldName);
          if f.DataType = ftBoolean then
          begin
            v := AReader.FieldByName(field.FieldName).AsBoolean
          end
          else
          begin
            v := AReader.FieldByName(field.FieldName).AsInteger <> 0;
          end;
          S := field.FieldName + ' as boolean';
        end
        else if CompareText(field.FieldType, 'datetime') = 0 then
        begin
          f := AReader.FieldByName(field.FieldName);
          if f.IsNull then begin
            Continue;
          end else begin
            v := f.AsDateTime;
            S := field.FieldName + ' as datetime';
          end;
        end
        else if CompareText(field.FieldType, 'time') = 0 then
        begin
          f := AReader.FieldByName(field.FieldName);
          if f.IsNull then begin
            Continue;
          end else begin
            v := AReader.FieldByName(field.FieldName).AsDateTime;
            S := field.FieldName + ' as datetime';
          end;
        end
        else
          raise Exception.Create('Unknown field type for ' + field.FieldName);
        try
          TdormUtils.SetField(obj, field.name, v);
        except
          on E: Exception do
          begin
            raise EdormException.Create(E.Message + sLineBreak + '. Probably cannot write ' + ARttiType.ToString
              + '.' + S);
          end;
        end;
      end;
      for i := 0 to AMappingTable.JoinTableList.Count - 1 do begin
        v := TdormUtils.GetProperty(obj, AMappingTable.JoinTableList[i].PropName);
        if v.IsObject and assigned (AMappingTable.JoinTableList[i].MappingTable) then begin
          subobj := v.AsObject;
          if assigned(subobj) then begin
            LoadObjectFromFireDACReader(subobj, ctx.GetType(Obj.ClassType), AReader, AMappingTable.JoinTableList[i].MappingTable.Fields, AMappingTable.JoinTableList[i].Qualifier);
          end;
        end;
      end;
    except
      on E: Exception do
      begin
        raise;
      end;
    end;
    Result := obj;
  finally
    ctx.Free;
  end;
end;

class procedure TFireDACBaseAdapter.register;
begin
  //
end;

procedure TFireDACBaseAdapter.Rollback;
begin
  FD.GetCurrentTransaction.Rollback;
end;

procedure TFireDACBaseAdapter.SetFireDACParameterValue(AField: TMappingField; AStatement: TFDQuery;
  AValue: TValue; AIsNullable: boolean);
var
  sourceStream: TStream;
  str: TStringStream;
  StrVal : string;
  Param : TFDParam;
begin
  Param := AStatement.ParamByName(AField.FieldName);
  if assigned(Param) then begin
    if CompareText(AField.FieldType, 'string') = 0 then
    begin
      if AField.Size > 0 then begin
        StrVal := AValue.AsString.Substring(0, AField.Size);
      end else begin
        StrVal := AValue.AsString;
      end;
      if ByteLength(StrVal) > 8000 then begin
        Param.DataType := ftWideMemo;
        Param.AsWideMemo := StrVal;
      end else begin
        if AStatement.Command.UnicodeDB then begin
          Param.DataType := ftWideString;
        end else begin
          Param.DataType := ftString;
        end;
        Param.AsString := StrVal;
      end;
      GetLogger.Debug(Param.Name + ' = ' + StrVal);
    end
    else if CompareText(AField.FieldType, 'decimal') = 0 then
    begin
      if (AValue.AsExtended = 0.0) and AIsNullable then
        SetNullParameterValue(Param)
      else
      begin
        Param.DataType := ftFloat;
        Param.AsFloat := AValue.AsExtended;
        GetLogger.Debug(Param.Name + ' = ' + FloatToStr(AValue.AsExtended));
      end;
    end
    else if CompareText(AField.FieldType, 'float') = 0 then
    begin
      if (AValue.AsExtended = 0.0) and AIsNullable then
        SetNullParameterValue(Param)
      else
      begin
        Param.DataType := ftFloat;
        Param.AsFloat := AValue.AsExtended;
        GetLogger.Debug(Param.Name + ' = ' + FloatToStr(AValue.AsExtended));
      end;
    end
    else if (CompareText(AField.FieldType, 'integer') = 0) or (CompareText(AField.FieldType, 'int64') = 0) then
    begin
      if (AValue.AsInt64 = 0) and AIsNullable then
        SetNullParameterValue(Param)
      else
      begin
        Param.DataType := ftLargeint;
        Param.AsLargeInt := AValue.AsInt64;
        GetLogger.Debug(Param.Name + ' = ' + IntToStr(AValue.AsInt64));
      end;
    end
    else if CompareText(AField.FieldType, 'boolean') = 0 then
    begin
      Param.DataType := ftBoolean;
      Param.AsBoolean := AValue.AsBoolean;
      GetLogger.Debug(Param.Name + ' = ' + BoolToStr(AValue.AsBoolean, true));
    end
    else if CompareText(AField.FieldType, 'date') = 0 then
    begin
      if (AValue.AsExtended = 0) and AIsNullable then
        SetNullParameterValue(Param)
      else
      begin
        Param.DataType := ftDate;
        Param.AsDate := trunc(AValue.AsExtended);
        GetLogger.Debug(Param.Name + ' = ' + EscapeDate(trunc(AValue.AsExtended)));
      end;
    end
    else if CompareText(AField.FieldType, 'datetime') = 0 then
    begin
      if (AValue.AsExtended = 0) and AIsNullable then
        SetNullParameterValue(Param)
      else
      begin
        Param.DataType := ftDateTime;
        Param.AsDateTime := AValue.AsExtended;
        GetLogger.Debug(Param.Name + ' = ' + EscapeDateTime(AValue.AsExtended, true));
      end;
    end
    else if CompareText(AField.FieldType, 'time') = 0 then
    begin
      if (AValue.AsExtended = 0) and AIsNullable then
        SetNullParameterValue(Param)
      else
      begin
        Param.DataType := ftTime;
        Param.AsDateTime := AValue.AsExtended;
        GetLogger.Debug(Param.Name + ' = ' + EscapeDateTime(AValue.AsExtended));
      end;
    end
    else if CompareText(AField.FieldType, 'blob') = 0 then
    begin
      sourceStream := TStream(AValue.AsObject);
      if sourceStream = nil then
      begin
        Param.AsBlob := '';
        GetLogger.Debug(Param.Name + ' = nil');
      end
      else
      begin
        str := TStringStream.Create;
        try
          sourceStream.Position := 0;
          str.CopyFrom(sourceStream, 0);
          str.Position := 0;
          Param.AsBlob := str.DataString;
          GetLogger.Debug(Param.Name + ' = <blob ' + IntToStr(str.Size) + ' bytes>');
        finally
          str.Free;
        end;
      end;
    end
    else
      raise EdormException.CreateFmt('Parameter type not supported: [%s]', [AField.FieldType]);
  end else begin
    raise EdormException.CreateFmt('Parameter not found: [%s]', [AField.FieldName]);
  end;
end;

procedure TFireDACBaseAdapter.SetNullParameterValue(AParam: TFDParam);
begin
  AParam.DataType := ftString;
  AParam.AsString := '';
  GetLogger.Debug(AParam.Name + ' = ');
end;

procedure TFireDACBaseAdapter.SetLogger(ALogger: IdormLogger);
begin
  FLogger := ALogger;
end;

procedure TFireDACBaseAdapter.StartTransaction;
begin
  FD.GetConnection; // ensure database connected
  FD.StartTransaction;
end;

function TFireDACBaseAdapter.Load(ARttiType: TRttiType; AMappingTable: TMappingTable; const Value: TValue;
  AObject: TObject): boolean;
var
  reader: TFDQuery;
  ts : TDateTime;
begin
  reader := GetFireDACReaderFor(ARttiType, AMappingTable, Value);
  try
    ts := Now;
    try
      reader.Open();
      GetLogger.LogCall(reader.SQL.Text, ts);
      Result := not reader.Eof;
      if Result then
        LoadObjectFromFireDACReader(AObject, ARttiType, reader, AMappingTable.Fields);
    except
      on e : Exception do begin
        GetLogger.Error(e.Message, reader.SQL.Text);
        raise;
      end;
    end;
  finally
    reader.Free;
  end;
end;

end.
