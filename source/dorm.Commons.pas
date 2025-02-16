{ *******************************************************************************
  Copyright 2010-2015 Daniele Teti

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

unit dorm.Commons;

interface

uses
  RTTI,
  Classes,
  superobject,
  Generics.Collections,
  SysUtils,
  TypInfo,
  dorm.Mappings,
  dorm.Filters,
  dorm.Collections,
  dorm.Utils,
  dorm.Mappings.Strategies,
  dorm.ObjectStatus, System.Types, DB;

type
  TDormSaveMode = (os_Insert, os_Update, os_Delete, os_Upsert);
  TDuckTypedList = class;

  EdormException = class(Exception)

  end;

  EdormValidationException = class(EdormException)

  end;

  EdormLockingException = class(EdormException)

  end;

  TdormEnvironment = (deDevelopment, deTest, deRelease);
  TdormObjectOwner = (ooItself, ooParent);
  TdormSaveType = (stAllGraph, stSingleObject);
  TdormRelations = set of (drBelongsTo, drHasMany, drHasOne, drAll);

  TdormInterfacedObject = class(TInterfacedObject)
    constructor Create; virtual;
  end;

  TdormBaseObject = class(TObject)
  strict protected
    FObjStatus: TdormObjectStatus;
    procedure SetObjStatus(const Value: TdormObjectStatus);

  public
    function ObjStatusAsString: string;
    [Transient]
    property ObjStatus: TdormObjectStatus read FObjStatus write SetObjStatus;
  end;

  IdormLogger = interface
    ['{3501FE53-3781-4F6F-BE6C-80A2309D8D94}']
    procedure EnterLevel(const Value: string);
    procedure ExitLevel(const Value: string);
    procedure Error(const Value: string; const _SQL : string = '');
    procedure Warning(const Value: string);
    procedure Info(const Value: string);
    procedure Debug(const Value: string);
    procedure LogCall(const SQL: string; StartTime : TDateTime);
  end;

  IDataBaseBuilder = interface
    ['{71C310B8-FB56-40E6-AED4-D3104CDC9069}']
    procedure Execute;
  end;

  IdormCommand = interface
    ['{A403A53F-B0F0-4A35-B014-5EBFAABDE5D3}']
    function GetSQL: string;
  end;

  IList = interface
    ['{2A1BCB3C-17A2-4F8D-B6FB-32B2A1BFE840}']
    function Add(const Value: TObject): Integer;
    procedure Clear;
    function Count: Integer;
    function GetItem(index: Integer): TObject;
  end;

  IdormPersistStrategy = interface
    ['{04A7F5C2-9B9B-4259-90C2-F23894189717}']
    function Insert(ARttiType: TRttiType; AObject: TObject; AMappingTable: TMappingTable): TValue;
    function Update(ARttiType: TRttiType; AObject: TObject; AMappingTable: TMappingTable;
      ACurrentVersion: Int64): Int64;
    function Upsert(ARttiType: TRttiType; AObject: TObject; AMappingTable: TMappingTable): TValue;
    function Delete(ARttiType: TRttiType; AObject: TObject; AMappingTable: TMappingTable;
      ACurrentVersion: Int64): Int64;
    function Load(ARttiType: TRttiType; AMappingTable: TMappingTable; const Value: TValue;
      AObject: TObject): boolean; overload;
    function Load(ARttiType: TRttiType; AMappingTable: TMappingTable;
      AMappingRelationField: TMappingField; const Value: TValue; AObject: TObject; const DontRaiseExceptionOnUnexpectedMultiRowResult: Boolean)
      : boolean; overload;
    procedure DeleteAll(AMappingTable: TMappingTable);
    function Count(AMappingTable: TMappingTable): Int64;

    // function List(ARttiType: TRttiType; ATableName: string;
    // AMappingFields: TMappingFieldList;
    // AdormSearchCriteria: ICustomCriteria): TObjectList<TObject>; overload;
    // procedure FillList(AList: TObject; ARttiType: TRttiType;
    // ATableName: string; AMappingFields: TMappingFieldList;
    // AdormSearchCriteria: ICustomCriteria); overload;
    procedure LoadList(AList: TObject; ARttiType: TRttiType; AMappingTable: TMappingTable;
      ACriteria: ICriteria); overload;
    function GetLastInsertOID: TValue;
    procedure ConfigureStrategy(ConfigurationInfo: ISuperObject; const UnicodeDB: Boolean);
    procedure InitStrategy;
    procedure StartTransaction;
    procedure Commit;
    procedure Rollback;
    function InTransaction: boolean;
    procedure SetLogger(ALogger: IdormLogger);
    function RawExecute(SQL: string): Int64;
    function ExecuteAndGetFirst(SQL: string): Int64;
    function EscapeString(const Value: string): string;
    function EscapeDate(const Value: TDate): string;
    function EscapeDateTime(const Value: TDate; AWithMillisSeconds : boolean = false): string;
    function GetSelectSQL(ACriteria: ICriteria; AMappingTable: TMappingTable): string;
    function GetCountSQL(ACriteria: ICriteria; AMappingTable: TMappingTable): string;
    function GetDatabaseBuilder(AEntities: TList<string>; AMappings: ICacheMappingStrategy)
      : IDataBaseBuilder;
    function ExecuteCommand(ACommand: IdormCommand): Int64;
    procedure ExecStoredProcedure(const AProcName: String; _InputParams, _OutputParams: TStringList);
    function GetConnection : TCustomConnection;
    function GetFieldTypeAndSize(const _Table, _Field: string; out _Type: TFieldType; out _Size: integer): Boolean;
    function GetDBName: string;
    function GetEngine: string;
    function CanUpsert: Boolean;
  end;

  TdormListEnumerator = class(TEnumerator<TObject>)
  protected
    FPosition: Int64;
    FDuckTypedList: TDuckTypedList;

  protected
    function DoGetCurrent: TObject; override;
    function DoMoveNext: boolean; override;

  public
    constructor Create(ADuckTypedList: TDuckTypedList);
  end;

  IdormKeysGenerator = interface
    ['{476D88A6-E7A1-48A2-8673-C2B646A2E7F4}']
    function NewStringKey(const Entity: string): string;
    function NewIntegerKey(const Entity: string): UInt64;
    procedure SetPersistStrategy(const PersistentStrategy: IdormPersistStrategy);
  end;

  IWrappedList = interface
    ['{B60AF5A6-7C31-4EAA-8DFB-D8BD3E112EE7}']
    function Count: Integer;
    function GetItem(const index: Integer): TObject;
    procedure Add(const AObject: TObject);
    procedure Clear;
    function GetEnumerator: TdormListEnumerator;
  end;

  TDuckTypedList = class(TInterfacedObject, IWrappedList)
  protected
    FObjectAsDuck: TObject;
    FAddMethod: TRttiMethod;
    FClearMethod: TRttiMethod;
    FCountProperty: TRttiProperty;
    FGetItemMethod: TRttiMethod;
    FOwnsObject: boolean;
    function Count: Integer;
    function GetItem(const index: Integer): TObject;
    procedure Add(const AObject: TObject);
    procedure Clear;

  public
    constructor Create(AObjectAsDuck: TObject; AOwnsObject: boolean);
    destructor Destroy; override;
    function GetEnumerator: TdormListEnumerator;
  end;

  TdormValidateable = class abstract
  public
    // Called at every validation. NEED TO BE INHERITED IN CHILD CLASSES
    procedure Validate; virtual; abstract;
    // Called after "Validate" only while inserting
    procedure InsertValidate; virtual; abstract;
    // Called after "Validate" only while Updating
    procedure UpdateValidate; virtual; abstract;
    // Called after "Validate" only while Deleting
    procedure DeleteValidate; virtual; abstract;
    // Called after "Validate" only while Upserting
    procedure UpsertValidate; virtual; abstract;

    // Events related method
    procedure OnBeforeLoad; virtual; abstract;
    procedure OnAfterLoad; virtual; abstract;

    procedure OnBeforePersist; virtual; abstract;
    procedure OnAfterPersist; virtual; abstract;

    procedure OnBeforeInsert; virtual; abstract;
    procedure OnAfterInsert; virtual; abstract;

    procedure OnBeforeUpdate; virtual; abstract;
    procedure OnAfterUpdate; virtual; abstract;

    procedure OnBeforeDelete; virtual; abstract;
    procedure OnAfterDelete; virtual; abstract;

    procedure OnBeforeUpsert; virtual; abstract;
    procedure OnAfterUpsert; virtual; abstract;
  end;

  TDuckTypedObject = class(TdormValidateable)
  strict private
    FLastWrapperClassType: TClass;

  private
    FObjectAsDuck: TObject;
    _type: TRttiType;
    FValidate: TRttiMethod;
    FInsertValidate: TRttiMethod;
    FUpdateValidate: TRttiMethod;
    FDeleteValidate: TRttiMethod;
    FUpsertValidate: TRttiMethod;
    FOnBeforeLoad: TRttiMethod;
    FOnAfterLoad: TRttiMethod;

    FOnBeforePersist: TRttiMethod;
    FOnAfterPersist: TRttiMethod;

    FOnBeforeInsert: TRttiMethod;
    FOnAfterInsert: TRttiMethod;

    FOnBeforeUpdate: TRttiMethod;
    FOnAfterUpdate: TRttiMethod;

    FOnBeforeDelete: TRttiMethod;
    FOnAfterDelete: TRttiMethod;

    FOnBeforeUpsert: TRttiMethod;
    FOnAfterUpsert: TRttiMethod;

    procedure BindValidatingMethods(AType: TRttiType);
    procedure BindEventsMethods(AType: TRttiType);

  strict protected
    procedure InitializeDuckedInterface;
    function SafeMethodCall(AMethod: TRttiMethod; AObject: TObject): boolean;

  public
    // Called at every validation. NEED TO BE IMPLEMENTED IF VALIDATION IS NEEDED
    procedure Validate; override;
    // Called after "Validate" only while inserting
    procedure InsertValidate; override;
    // Called after "Validate" only while Updating
    procedure UpdateValidate; override;
    // Called after "Validate" only while Deleting
    procedure DeleteValidate; override;
    // Called after "Validate" only while upserting
    procedure UpsertValidate; override;
    procedure OnAfterLoad; override;
    procedure OnBeforeLoad; override;

    procedure OnBeforePersist; override;
    procedure OnAfterPersist; override;

    procedure OnBeforeInsert; override;
    procedure OnAfterInsert; override;

    procedure OnBeforeUpdate; override;
    procedure OnAfterUpdate; override;

    procedure OnBeforeDelete; override;
    procedure OnAfterDelete; override;

    procedure OnBeforeUpsert; override;
    procedure OnAfterUpsert; override;

  public
    constructor Create;
    function WrapObject(AObject: TObject): TdormValidateable;
    destructor Destroy; override;
  end;

function GetPKMappingIndex(const AMappingFields: TMappingFieldList): Integer;
function GetPKMappingIndexes(const AMappingFields: TMappingFieldList): TIntegerDynArray;
function GetMappingRelationByPropertyName(ARelationList: TMappingRelationList;
  const APropertyName: string): TMappingRelation;
function GetMappingBelongsToByPropertyName(ARelationList: TMappingBelongsToList;
  const APropertyName: string): TMappingBelongsTo;
function GetSelectFieldsList(AMapping: TMappingFieldList; AWithPrimaryKey: boolean): string;

function WrapAsList(const AObject: TObject; AOwnsObject: boolean = false): IWrappedList;
function WrapAsValidateableObject(AObject: TObject; AValidateableDuckObject: TDuckTypedObject = nil)
  : TdormValidateable;

implementation

uses dorm.loggers;

function WrapAsValidateableObject(AObject: TObject; AValidateableDuckObject: TDuckTypedObject = nil)
  : TdormValidateable;
begin
  if Assigned(AValidateableDuckObject) then
    Result := AValidateableDuckObject.WrapObject(AObject)
  else
  begin
    Result := TDuckTypedObject.Create;
    TDuckTypedObject(Result).WrapObject(AObject);
  end;
end;

function GetSelectFieldsList(AMapping: TMappingFieldList; AWithPrimaryKey: boolean): string;
var
  field: TMappingField;
begin
  Result := '';
  if AWithPrimaryKey then
    for field in AMapping do
    begin
      Result := Result + ',"' + field.FieldName + '"';
    end
  else
    for field in AMapping do
    begin
      if not field.IsPK then
        Result := Result + ',"' + field.FieldName + '"';
    end;
  System.Delete(Result, 1, 1);
end;

function GetMappingRelationByPropertyName(ARelationList: TMappingRelationList;
  const APropertyName: string): TMappingRelation;
var
  _relation: TMappingRelation;
begin
  Result := nil;
  for _relation in ARelationList do
    if CompareText(_relation.Name, APropertyName) = 0 then
    begin
      Result := _relation;
      Break;
    end;
end;

function GetMappingBelongsToByPropertyName(ARelationList: TMappingBelongsToList;
  const APropertyName: string): TMappingBelongsTo;
var
  _relation: TMappingBelongsTo;
begin
  Result := nil;
  for _relation in ARelationList do
    if CompareText(_relation.Name, APropertyName) = 0 then
    begin
      Result := _relation;
      Break;
    end;
end;

function GetPKMappingIndex(const AMappingFields: TMappingFieldList): Integer;
var
  I: Integer;
begin
  for I := 0 to AMappingFields.Count - 1 do
    if AMappingFields[I].IsPK then
      Exit(I);
  Exit(-1);
end;

function GetPKMappingIndexes(const AMappingFields: TMappingFieldList): TIntegerDynArray;
var
  I: Integer;
begin
  for I := 0 to AMappingFields.Count - 1 do begin
    if AMappingFields[I].IsPK then begin
      SetLength(Result, Length(Result) + 1);
      Result[Length(Result) - 1] := I;
    end;
  end;
end;

{ TdormInterfacedObject }

constructor TdormInterfacedObject.Create;
begin
  inherited Create;
end;

constructor TdormListEnumerator.Create(ADuckTypedList: TDuckTypedList);
begin
  inherited Create;
  FDuckTypedList := ADuckTypedList;
  FPosition := -1;
end;

function TdormListEnumerator.DoGetCurrent: TObject;
begin
  if FPosition > -1 then
    Result := FDuckTypedList.GetItem(FPosition)
  else
    raise Exception.Create('Enumerator error: Call MoveNext first');
end;

function TdormListEnumerator.DoMoveNext: boolean;
begin
  if FPosition < FDuckTypedList.Count - 1 then
  begin
    Inc(FPosition);
    Result := true;
  end
  else
    Result := false;
end;

function TDuckTypedList.GetEnumerator: TdormListEnumerator;
begin
  Result := TdormListEnumerator.Create(self);
end;

procedure TDuckTypedList.Add(const AObject: TObject);
begin
  FAddMethod.Invoke(FObjectAsDuck, [AObject]);
end;

procedure TDuckTypedList.Clear;
begin
  FClearMethod.Invoke(FObjectAsDuck, []);
end;

function TDuckTypedList.Count: Integer;
begin
  Result := FCountProperty.GetValue(FObjectAsDuck).AsInteger;
end;

constructor TDuckTypedList.Create(AObjectAsDuck: TObject; AOwnsObject: boolean);
var ctx: TRttiContext;
begin
  inherited Create;
  FOwnsObject := AOwnsObject;
  FObjectAsDuck := AObjectAsDuck;
  ctx:= TRttiContext.Create;
  try
    FAddMethod := ctx.GetType(AObjectAsDuck.ClassInfo).GetMethod('Add');
    if not Assigned(FAddMethod) then
      raise EdormException.Create('Cannot find method "Add" in the duck object');
    FClearMethod := ctx.GetType(AObjectAsDuck.ClassInfo).GetMethod('Clear');
    if not Assigned(FClearMethod) then
      raise EdormException.Create('Cannot find method "Clear" in the duck object');
    FGetItemMethod := nil;

  {$IF CompilerVersion >= 23}
    FGetItemMethod := ctx.GetType(AObjectAsDuck.ClassInfo).GetIndexedProperty('Items')
      .ReadMethod;

  {$ENDIF}
    if not Assigned(FGetItemMethod) then
      FGetItemMethod := ctx.GetType(AObjectAsDuck.ClassInfo).GetMethod('GetItem');
    if not Assigned(FGetItemMethod) then
      FGetItemMethod := ctx.GetType(AObjectAsDuck.ClassInfo).GetMethod('GetElement');
    if not Assigned(FGetItemMethod) then
      raise EdormException.Create
        ('Cannot find method Indexed property "Items" or method "GetItem" or method "GetElement" in the duck object');
    FCountProperty := ctx.GetType(AObjectAsDuck.ClassInfo).GetProperty('Count');
    if not Assigned(FCountProperty) then
      raise EdormException.Create('Cannot find property "Count" in the duck object');
  finally
    ctx.Free;
  end;
end;

destructor TDuckTypedList.Destroy;
begin
  if FOwnsObject then
    FreeAndNil(FObjectAsDuck);
  inherited;
end;

function TDuckTypedList.GetItem(const index: Integer): TObject;
begin
  Result := FGetItemMethod.Invoke(FObjectAsDuck, [index]).AsObject;
end;

function WrapAsList(const AObject: TObject; AOwnsObject: boolean): IWrappedList;
begin
  Result := TDuckTypedList.Create(AObject, AOwnsObject);
end;

{ TdormBaseObject }

function TdormBaseObject.ObjStatusAsString: string;
begin
  case FObjStatus of
    osDirty:
      Result := 'dirty';
    osClean:
      Result := 'clean';
    osUnknown:
      Result := 'unknown';
    osDeleted:
      Result := 'deleted';
  end;
end;

procedure TdormBaseObject.SetObjStatus(const Value: TdormObjectStatus);
begin
  FObjStatus := Value;
end;

{ TDuckTypedObject }

constructor TDuckTypedObject.Create;
begin
  inherited;
end;

procedure TDuckTypedObject.DeleteValidate;
begin
  SafeMethodCall(FDeleteValidate, FObjectAsDuck);
end;

destructor TDuckTypedObject.Destroy;
begin

  inherited;
end;

procedure TDuckTypedObject.BindEventsMethods(AType: TRttiType);
var
  _Method: TRttiMethod;
begin
  {
    //Events related method
    procedure OnBeforeLoad; virtual; abstract;
    procedure OnAfterLoad; virtual; abstract;

    procedure OnBeforePersist; virtual; abstract;
    procedure OnAfterPersist; virtual; abstract;

    procedure OnBeforeInsert; virtual; abstract;
    procedure OnAfterInsert; virtual; abstract;

    procedure OnBeforeUpdate; virtual; abstract;
    procedure OnAfterUpdate; virtual; abstract;

    procedure OnBeforeDelete; virtual; abstract;
    procedure OnAfterDelete; virtual; abstract;

  }

  _Method := _type.GetMethod('OnBeforeLoad');
  if Assigned(_Method) and (Length(_Method.GetParameters) = 0) then
    FOnBeforeLoad := _Method
  else
    FOnBeforeLoad := nil;

  _Method := _type.GetMethod('OnAfterLoad');
  if Assigned(_Method) and (Length(_Method.GetParameters) = 0) then
    FOnAfterLoad := _Method
  else
    FOnAfterLoad := nil;

  _Method := _type.GetMethod('OnBeforePersist');
  if Assigned(_Method) and (Length(_Method.GetParameters) = 0) then
    FOnBeforePersist := _Method
  else
    FOnBeforePersist := nil;

  _Method := _type.GetMethod('OnAfterPersist');
  if Assigned(_Method) and (Length(_Method.GetParameters) = 0) then
    FOnAfterPersist := _Method
  else
    FOnAfterPersist := nil;

  _Method := _type.GetMethod('OnBeforeInsert');
  if Assigned(_Method) and (Length(_Method.GetParameters) = 0) then
    FOnBeforeInsert := _Method
  else
    FOnBeforeInsert := nil;

  _Method := _type.GetMethod('OnAfterInsert');
  if Assigned(_Method) and (Length(_Method.GetParameters) = 0) then
    FOnAfterInsert := _Method
  else
    FOnAfterInsert := nil;

  _Method := _type.GetMethod('OnBeforeUpdate');
  if Assigned(_Method) and (Length(_Method.GetParameters) = 0) then
    FOnBeforeUpdate := _Method
  else
    FOnBeforeUpdate := nil;

  _Method := _type.GetMethod('OnAfterUpdate');
  if Assigned(_Method) and (Length(_Method.GetParameters) = 0) then
    FOnAfterUpdate := _Method
  else
    FOnAfterUpdate := nil;

  _Method := _type.GetMethod('OnBeforeDelete');
  if Assigned(_Method) and (Length(_Method.GetParameters) = 0) then
    FOnBeforeDelete := _Method
  else
    FOnBeforeDelete := nil;

  _Method := _type.GetMethod('OnAfterDelete');
  if Assigned(_Method) and (Length(_Method.GetParameters) = 0) then
    FOnAfterDelete := _Method
  else
    FOnAfterDelete := nil;

  _Method := _type.GetMethod('OnBeforeUpsert');
  if Assigned(_Method) and (Length(_Method.GetParameters) = 0) then
    FOnBeforeUpsert := _Method
  else
    FOnBeforeUpsert := nil;

  _Method := _type.GetMethod('OnAfterUpsert');
  if Assigned(_Method) and (Length(_Method.GetParameters) = 0) then
    FOnAfterUpsert := _Method
  else
    FOnAfterUpsert := nil;
end;

procedure TDuckTypedObject.BindValidatingMethods(AType: TRttiType);
var
  _Method: TRttiMethod;
begin
  _Method := _type.GetMethod('Validate');
  if Assigned(_Method) and (not((Length(_Method.GetParameters) <> 0))) then
    FValidate := _Method
  else
    FValidate := nil;

  _Method := _type.GetMethod('InsertValidate');
  if Assigned(_Method) and (not((Length(_Method.GetParameters) <> 0))) then
    FInsertValidate := _Method
  else
    FInsertValidate := nil;

  _Method := _type.GetMethod('UpdateValidate');
  if Assigned(_Method) and (not((Length(_Method.GetParameters) <> 0))) then
    FUpdateValidate := _Method
  else
    FUpdateValidate := nil;

  _Method := _type.GetMethod('DeleteValidate');
  if Assigned(_Method) and (not((Length(_Method.GetParameters) <> 0))) then
    FDeleteValidate := _Method
  else
    FDeleteValidate := nil;

  _Method := _type.GetMethod('UpsertValidate');
  if Assigned(_Method) and (not((Length(_Method.GetParameters) <> 0))) then
    FUpsertValidate := _Method
  else
    FUpsertValidate := nil;
end;

procedure TDuckTypedObject.InitializeDuckedInterface;
var ctx : TRttiContext;
begin
  ctx:= TRttiContext.Create;
  try
    _type := ctx.GetType(FLastWrapperClassType);
    BindValidatingMethods(_type);
    BindEventsMethods(_type);
  finally
    ctx.Free;
  end;
end;

procedure TDuckTypedObject.InsertValidate;
begin
  SafeMethodCall(FInsertValidate, FObjectAsDuck);
end;

procedure TDuckTypedObject.OnAfterDelete;
begin
  SafeMethodCall(FOnAfterDelete, FObjectAsDuck);
end;

procedure TDuckTypedObject.OnAfterInsert;
begin
  SafeMethodCall(FOnAfterInsert, FObjectAsDuck);
end;

procedure TDuckTypedObject.OnAfterLoad;
begin
  SafeMethodCall(FOnAfterLoad, FObjectAsDuck);
end;

procedure TDuckTypedObject.OnAfterPersist;
begin
  SafeMethodCall(FOnAfterPersist, FObjectAsDuck);
end;

procedure TDuckTypedObject.OnAfterUpdate;
begin
  SafeMethodCall(FOnAfterUpdate, FObjectAsDuck);
end;

procedure TDuckTypedObject.OnAfterUpsert;
begin
  SafeMethodCall(FOnAfterUpsert, FObjectAsDuck);
end;

procedure TDuckTypedObject.OnBeforeDelete;
begin
  SafeMethodCall(FOnBeforeDelete, FObjectAsDuck);
end;

procedure TDuckTypedObject.OnBeforeInsert;
begin
  SafeMethodCall(FOnBeforeInsert, FObjectAsDuck);
end;

procedure TDuckTypedObject.OnBeforeLoad;
begin
  SafeMethodCall(FOnBeforeLoad, FObjectAsDuck);
end;

procedure TDuckTypedObject.OnBeforePersist;
begin
  SafeMethodCall(FOnBeforePersist, FObjectAsDuck);
end;

procedure TDuckTypedObject.OnBeforeUpdate;
begin
  SafeMethodCall(FOnBeforeUpdate, FObjectAsDuck);
end;

procedure TDuckTypedObject.OnBeforeUpsert;
begin
  SafeMethodCall(FOnBeforeUpsert, FObjectAsDuck);
end;

function TDuckTypedObject.SafeMethodCall(AMethod: TRttiMethod; AObject: TObject): boolean;
begin
  Result := true;
  if Assigned(AMethod) then
    AMethod.Invoke(AObject, []);
end;

procedure TDuckTypedObject.UpdateValidate;
begin
  SafeMethodCall(FUpdateValidate, FObjectAsDuck);
end;

procedure TDuckTypedObject.UpsertValidate;
begin
  SafeMethodCall(FUpsertValidate, FObjectAsDuck);
end;

procedure TDuckTypedObject.Validate;
begin
  SafeMethodCall(FValidate, FObjectAsDuck);
end;

function TDuckTypedObject.WrapObject(AObject: TObject): TdormValidateable;
begin
  FObjectAsDuck := AObject;
  if FLastWrapperClassType <> FObjectAsDuck.ClassType then
  begin
    FLastWrapperClassType := FObjectAsDuck.ClassType;
    InitializeDuckedInterface;
  end;
  Result := self;
end;

end.
