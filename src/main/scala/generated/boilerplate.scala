package generated 
import caliban._
import caliban.GraphQL.graphQL
import caliban.execution.Field
import caliban.schema.{ArgBuilder, GenericSchema}
import caliban.schema.Schema.gen
import caliban.schema.Annotations.GQLDirective
import caliban.federation._
import zio.Has
import zio.clock.Clock
import zio.console.Console

import java.time.ZonedDateTime
import java.util.UUID
object Whatever {
// Models 
case class lineitems(orderid: UUID,product_id: String,amount: Int,orders: orders,product: product)
@GQLDirective(Key("id")) case class orders(id: UUID,buyer: UUID,ordered_at: ZonedDateTime,lineitems: List[lineitems],users: users)
@GQLDirective(Key("upc")) case class product(upc: String,name: Option[String],price: Option[Int],weight: Option[Int],lineitems: List[lineitems],review: List[review])
case class review(author: UUID,product: String,body: Option[String],product_ref: product,users: users)
@GQLDirective(Key("id")) case class users(id: UUID,name: Option[String],age: Option[Int],email: Option[String],orders: List[orders],review: List[review])
// Output types 
case class MutateordersResponse(affected_rows: Int,rows: orders)
case class MutateproductResponse(affected_rows: Int,rows: product)
case class MutateusersResponse(affected_rows: Int,rows: users)
// Arg code 
case class ordersPK(id: UUID)
case class CreateordersInput(id: UUID,buyer: UUID,ordered_at: ZonedDateTime,lineitems: List[lineitems],users: users)
case class UpdateordersArgs(pk: ordersPK,set: SetordersFields)
case class productPK(upc: String)
case class CreateproductInput(upc: String,name: Option[String],price: Option[Int],weight: Option[Int],lineitems: List[lineitems],review: List[review])
case class UpdateproductArgs(pk: productPK,set: SetproductFields)
case class usersPK(id: UUID)
case class CreateusersInput(id: UUID,name: Option[String],age: Option[Int],email: Option[String],orders: List[orders],review: List[review])
case class UpdateusersArgs(pk: usersPK,set: SetusersFields)
case class SetordersFields(buyer: UUID,ordered_at: ZonedDateTime,lineitems: List[lineitems],users: users)
case class SetproductFields(name: Option[String],price: Option[Int],weight: Option[Int],lineitems: List[lineitems],review: List[review])
case class SetusersFields(name: Option[String],age: Option[Int],email: Option[String],orders: List[orders],review: List[review])
// QueryRoot 
case class Queries (
	get_orders_by_id: (Field) =>  (ordersPK) => Option[orders], 
	get_product_by_id: (Field) =>  (productPK) => Option[product], 
	get_users_by_id: (Field) =>  (usersPK) => Option[users]
)
// MutationRoot 
case class Mutations(
	delete_orders: (Field) =>  (ordersPK) => orders, 
	insert_orders: (Field) =>  (CreateordersInput) => orders, 
	update_orders: (Field) =>  (UpdateordersArgs) => MutateordersResponse, 
	delete_product: (Field) =>  (productPK) => product, 
	insert_product: (Field) =>  (CreateproductInput) => product, 
	update_product: (Field) =>  (UpdateproductArgs) => MutateproductResponse, 
	delete_users: (Field) =>  (usersPK) => users, 
	insert_users: (Field) =>  (CreateusersInput) => users, 
	update_users: (Field) =>  (UpdateusersArgs) => MutateusersResponse
)

// Service definition 

type ZIOTestService = Has[TestService]

 trait TestService {
	 def delete_orders(field: codegen.Field,pk: ordersPK): orders
	 def insert_orders(field: codegen.Field,objectO: CreateordersInput): orders
	 def update_orders(field: codegen.Field,arg: UpdateordersArgs): MutateordersResponse
	 def get_orders_by_id(field: codegen.Field,pk: ordersPK): Option[orders]
	 def delete_product(field: codegen.Field,pk: productPK): product
	 def insert_product(field: codegen.Field,objectO: CreateproductInput): product
	 def update_product(field: codegen.Field,arg: UpdateproductArgs): MutateproductResponse
	 def get_product_by_id(field: codegen.Field,pk: productPK): Option[product]
	 def delete_users(field: codegen.Field,pk: usersPK): users
	 def insert_users(field: codegen.Field,objectO: CreateusersInput): users
	 def update_users(field: codegen.Field,arg: UpdateusersArgs): MutateusersResponse
	 def get_users_by_id(field: codegen.Field,pk: usersPK): Option[users]
 }

val schema = new GenericSchema[ZIOTestService]{}
import schema._

 def buildApi(query: Queries, mutation: Mutations) : GraphQL[Console with Clock with ZIOTestService] =  {
   val api = graphQL(
     RootResolver(
       query,
       mutation
     )
   )
   api
 }



}