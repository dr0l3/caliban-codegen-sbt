//package codegen2
//
//
//import caliban._
//import caliban.GraphQL.graphQL
//import caliban.execution.Field
//import caliban.schema.{ArgBuilder, GenericSchema}
//import caliban.schema.Schema.gen
//import caliban.schema._
//import caliban.schema.Annotations.GQLDirective
//import caliban.InputValue.ObjectValue
//import caliban.InputValue
//import caliban.federation._
//import caliban.introspection.adt.{__Directive, __Type}
//import caliban.wrappers.Wrapper
//import caliban.parsing.adt.Type
//import zio.ZIO
//import zio.query.ZQuery
//import io.circe.Json
//import io.circe.generic.auto._
//import io.circe.syntax._
//import codegen.{Column, ExportedKeyInfo, ObjectExtension, ObjectExtensionField, Table}
//import generic.{DeleteInput, FederationArg, InsertInput, UpdateInput}
//import generic.Util._
//
//import java.sql.Connection
//import java.time.ZonedDateTime
//import java.util.UUID
//
//
//case class lineitems(orderid: UUID,product_id: String,amount: Int,orders: orders,product: product)
//@GQLDirective(Key("id")) case class orders(id: UUID,buyer: UUID,ordered_at: ZonedDateTime,lineitems: List[lineitems],users: users)
//@GQLDirective(Key("upc")) case class product(upc: String,name: Option[String],price: Option[Int],weight: Option[Int],lineitems: List[lineitems],review: List[review])
//case class review(author: UUID,product: String,body: Option[String],product_ref: product,users: users)
//@GQLDirective(Key("id")) case class users(id: UUID,name: Option[String],age: Option[Int],email: Option[String],orders: List[orders],review: List[review], user: User)
//
//@GQLDirective(Key("id")) case class usersaddressExtensionArgs(id: UUID)
//
//@GQLDirective(Key("id")) @GQLDirective(Extend)case class User(@GQLDirective(External)id: UUID)
//
//case class ordersFederationArg(id: UUID) extends FederationArg{
// def toSelect(): String =  {
//  s"id = '$id'"
// }
//}
//
//case class productFederationArg(upc: String) extends FederationArg{
// def toSelect(): String =  {
//  s"upc = '$upc'"
// }
//}
//
//case class usersFederationArg(id: UUID) extends FederationArg{
// def toSelect(): String =  {
//  s"id = '$id'"
// }
//}
//
//
//case class MutateordersResponse(affected_rows: Int,rows: orders)
//case class MutateproductResponse(affected_rows: Int,rows: product)
//case class MutateusersResponse(affected_rows: Int,rows: users)
//
//case class UpdateordersArgs(pk: ordersPK,set: SetordersFields) extends UpdateInput{
// def toUpdate(): String = {
//  ???
// }
//}
//
//case class SetproductFields(name: Option[String],price: Option[Int],weight: Option[Int])
//@GQLDirective(Key("id")) case class usersPK(id: UUID) extends DeleteInput{
// def toDelete(): String = {
//  ???
// }
//}
//
//case class UpdateusersArgs(pk: usersPK,set: SetusersFields) extends UpdateInput{
// def toUpdate(): String = {
//  ???
// }
//}
//
//case class UpdateproductArgs(pk: productPK,set: SetproductFields) extends UpdateInput{
// def toUpdate(): String = {
//  ???
// }
//}
//
//case class SetusersFields(name: Option[String],age: Option[Int],email: Option[String])
//@GQLDirective(Key("id")) case class ordersPK(id: UUID) extends DeleteInput{
// def toDelete(): String = {
//  ???
// }
//}
//
//@GQLDirective(Key("upc")) case class productPK(upc: String) extends DeleteInput{
// def toDelete(): String = {
//  ???
// }
//}
//
//case class SetordersFields(buyer: UUID,ordered_at: ZonedDateTime,users: users)
//@GQLDirective(Key("upc")) case class CreateproductInput(upc: String,name: Option[String],price: Option[Int],weight: Option[Int]) extends InsertInput{
// def toInsert(): String = {
//  s"insert into product(upc,name,price,weight) values ('$upc','$name',$price,$weight)"
// }
//}
//
//@GQLDirective(Key("id")) case class CreateordersInput(id: UUID,buyer: UUID,ordered_at: ZonedDateTime,users: users) extends InsertInput{
// def toInsert(): String = {
//  s"insert into orders(id,buyer,ordered_at,users) values ('$id','$buyer','$ordered_at',$users)"
// }
//}
//
//@GQLDirective(Key("id")) case class CreateusersInput(id: UUID,name: Option[String],age: Option[Int],email: Option[String]) extends InsertInput{
// def toInsert(): String = {
//  s"insert into users(id,name,age,email) values ('$id','$name',$age,'$email')"
// }
//}
//
//
//case class Queries (
//	get_orders_by_id: (Field) =>  (ordersPK) => Option[orders],
//	get_product_by_id: (Field) =>  (productPK) => Option[product],
//	get_users_by_id: (Field) =>  (usersPK) => Option[users]
//)
//
//case class Mutations(
//	delete_orders: (Field) =>  (ordersPK) => orders,
//	insert_orders: (Field) =>  (CreateordersInput) => orders,
//	update_orders: (Field) =>  (UpdateordersArgs) => MutateordersResponse,
//	delete_product: (Field) =>  (productPK) => product,
//	insert_product: (Field) =>  (CreateproductInput) => product,
//	update_product: (Field) =>  (UpdateproductArgs) => MutateproductResponse,
//	delete_users: (Field) =>  (usersPK) => users,
//	insert_users: (Field) =>  (CreateusersInput) => users,
//	update_users: (Field) =>  (UpdateusersArgs) => MutateusersResponse
//)
//
//
//trait Extensions {
// def usersaddress(field: Field, args: usersaddressExtensionArgs): ZIO[Any,Throwable, String]
//}
//
//object EntityResolvers {
// def createordersEntityResolver(extensions: ExtensionLogicMap, tables: List[Table], conditionsHandlerMap: Map[String, ObjectValue => String], connection: Connection)(implicit schema: Schema[Any,orders]): EntityResolver[Any] = new EntityResolver[Any] {
//    override def resolve(value: InputValue): ZQuery[Any, CalibanError, Step[Any]] = genericEntityHandler(extensions, tables, conditionsHandlerMap, connection)(value)
//    override def toType: __Type = extendType(schema.toType_(), extensions.keys.toList)
//}
//def createproductEntityResolver(extensions: ExtensionLogicMap, tables: List[Table], conditionsHandlerMap: Map[String, ObjectValue => String], connection: Connection)(implicit schema: Schema[Any,product]): EntityResolver[Any] = new EntityResolver[Any] {
//    override def resolve(value: InputValue): ZQuery[Any, CalibanError, Step[Any]] = genericEntityHandler(extensions, tables, conditionsHandlerMap, connection)(value)
//    override def toType: __Type = extendType(schema.toType_(), extensions.keys.toList)
//}
//def createusersEntityResolver(extensions: ExtensionLogicMap, tables: List[Table], conditionsHandlerMap: Map[String, ObjectValue => String], connection: Connection)(implicit schema: Schema[Any,users]): EntityResolver[Any] = new EntityResolver[Any] {
//    override def resolve(value: InputValue): ZQuery[Any, CalibanError, Step[Any]] = genericEntityHandler(extensions, tables, conditionsHandlerMap, connection)(value)
//    override def toType: __Type = extendType(schema.toType_(), extensions.keys.toList)
//}
//}
//
//class API(extensions: Extensions, connection: Connection)(implicit val querySchema: Schema[Any, Queries], mutationSchema: Schema[Any, Mutations]) {
//   import EntityResolvers._
//   val tables = List(Table("lineitems", List(), List(Column("orderid", false, 1111, "uuid", false),Column("product_id", false, 12, "text", false),Column("amount", false, 4, "int4", false)), "null", List(ExportedKeyInfo("orders", "id", "lineitems", "orderid", 1, Some("orders_pkey"), Some("lineitems_orderid_fkey")),ExportedKeyInfo("product", "upc", "lineitems", "product_id", 1, Some("product_pkey"), Some("lineitems_product_id_fkey")))),Table("orders", List(Column("id", false, 1111, "uuid", true)), List(Column("buyer", false, 1111, "uuid", false),Column("ordered_at", false, 93, "timestamptz", false)), "null", List(ExportedKeyInfo("orders", "id", "lineitems", "orderid", 1, Some("orders_pkey"), Some("lineitems_orderid_fkey")),ExportedKeyInfo("users", "id", "orders", "buyer", 1, Some("users_pkey"), Some("orders_buyer_fkey")))),Table("product", List(Column("upc", false, 12, "text", true)), List(Column("name", true, 12, "text", false),Column("price", true, 4, "int4", false),Column("weight", true, 4, "int4", false)), "null", List(ExportedKeyInfo("product", "upc", "lineitems", "product_id", 1, Some("product_pkey"), Some("lineitems_product_id_fkey")),ExportedKeyInfo("product", "upc", "review", "product", 1, Some("product_pkey"), Some("review_product_fkey")))),Table("review", List(), List(Column("author", false, 1111, "uuid", false),Column("product", false, 12, "text", false),Column("body", true, 12, "text", false)), "null", List(ExportedKeyInfo("product", "upc", "review", "product", 1, Some("product_pkey"), Some("review_product_fkey")),ExportedKeyInfo("users", "id", "review", "author", 1, Some("users_pkey"), Some("review_author_fkey")))),Table("users", List(Column("id", false, 1111, "uuid", true)), List(Column("name", true, 12, "text", false),Column("age", true, 4, "int4", false),Column("email", true, 12, "text", true)), "null", List(ExportedKeyInfo("users", "id", "orders", "buyer", 1, Some("users_pkey"), Some("orders_buyer_fkey")),ExportedKeyInfo("users", "id", "review", "author", 1, Some("users_pkey"), Some("review_author_fkey")))))
//
//   val extensionLogicByType: ExtensionLogicMap = Map(
////      ObjectExtension("users", ObjectExtensionField("address", Type.NamedType("String", nonNull = true), Nil)) -> middleGenericJsonObject[usersaddressExtensionArgs,String](extensions.usersaddress)("address")
//   )
//
//   val insertByFieldName: Map[String, Json => String] = Map(
//      "insert_orders" -> convertInsertInput[CreateordersInput],
//"insert_product" -> convertInsertInput[CreateproductInput],
//"insert_users" -> convertInsertInput[CreateusersInput]
//   )
//
//   val updateByFieldName: Map[String, Json => String] = Map(
//
//   )
//
//   val deleteByFieldName: Map[String, Json => String] = Map(
//
//   )
//
//   val entityResolverConditionByTypeName: Map[String, ObjectValue => String]= Map(
//      "orders" -> convertFederationArg[ordersFederationArg],
//"product" -> convertFederationArg[productFederationArg],
//"users" -> convertFederationArg[usersFederationArg]
//   )
//
//  val baseApi = new GraphQL[Any] {
//    override protected val schemaBuilder: RootSchemaBuilder[Any] = RootSchemaBuilder(
//      Option(
//        constructQueryOperation[Any,Queries](
//          connection,
//          extensionLogicByType,
//          tables
//        )
//      ),
//      Option(
//        constructMutationOperation[Any, Mutations](
//          connection,
//          extensionLogicByType,
//          tables,
//          insertByFieldName,
//          updateByFieldName,
//          deleteByFieldName
//        )
//      ),
//      None
//    )
//    override protected val wrappers: List[Wrapper[Any]] = Nil
//    override protected val additionalDirectives: List[__Directive] = Nil
//  }
//
//    def createApi(
//               ): GraphQL[Any] = {
//    federate(
//      baseApi,createordersEntityResolver(extensionLogicByType, tables, entityResolverConditionByTypeName, connection), createproductEntityResolver(extensionLogicByType, tables, entityResolverConditionByTypeName, connection),createusersEntityResolver(extensionLogicByType, tables, entityResolverConditionByTypeName, connection)
//    )
//  }
//}
//
