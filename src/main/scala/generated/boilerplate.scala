package generated 
import caliban._
import caliban.GraphQL.graphQL
import caliban.execution.Field
import caliban.federation._
import caliban.schema.Annotations.GQLDirective
import caliban.schema.{ArgBuilder, GenericSchema}
import caliban.schema.Schema.gen
import zio.Has
import zio.clock.Clock
import zio.console.Console

import java.time.ZonedDateTime
import java.util.UUID
object Whatever {
// Models

case class lineitems(orderid: UUID,product_id: String,amount: Int,orders: orders,product: product)

	@GQLDirective(Key("id"))
case class orders(id: UUID,buyer: UUID,ordered_at: ZonedDateTime,lineitems: List[lineitems],users: users)
case class product(upc: String,name: Option[String],price: Option[Int],weight: Option[Int],lineitems: List[lineitems],review: List[review])
case class review(author: UUID,product: String,body: Option[String],product_ref: product,users: users)
case class users(id: UUID,name: Option[String],age: Option[Int],email: Option[String],orders: List[orders],review: List[review])
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
	delete_orders: (Field) =>  (ordersPK) => orders, 
	insert_orders: (Field) =>  (CreateordersInput) => orders, 
	update_orders: (Field) =>  (UpdateordersArgs) => MutateordersResponse, 
	get_orders_by_id: (Field) =>  (ordersPK) => Option[orders], 
	delete_product: (Field) =>  (productPK) => product, 
	insert_product: (Field) =>  (CreateproductInput) => product, 
	update_product: (Field) =>  (UpdateproductArgs) => MutateproductResponse, 
	get_product_by_id: (Field) =>  (productPK) => Option[product], 
	delete_users: (Field) =>  (usersPK) => users, 
	insert_users: (Field) =>  (CreateusersInput) => users, 
	update_users: (Field) =>  (UpdateusersArgs) => MutateusersResponse, 
	get_users_by_id: (Field) =>  (usersPK) => Option[users]
)
// Service definition 

type ZIOTestService = Has[TestService]

 trait TestService {
	 def delete_orders(field: Field,pk: ordersPK): orders
	 def insert_orders(field: Field,objectO: CreateordersInput): orders
	 def update_orders(field: Field,arg: UpdateordersArgs): MutateordersResponse
	 def get_orders_by_id(field: Field,pk: ordersPK): Option[orders]
	 def delete_product(field: Field,pk: productPK): product
	 def insert_product(field: Field,objectO: CreateproductInput): product
	 def update_product(field: Field,arg: UpdateproductArgs): MutateproductResponse
	 def get_product_by_id(field: Field,pk: productPK): Option[product]
	 def delete_users(field: Field,pk: usersPK): users
	 def insert_users(field: Field,objectO: CreateusersInput): users
	 def update_users(field: Field,arg: UpdateusersArgs): MutateusersResponse
	 def get_users_by_id(field: Field,pk: usersPK): Option[users]
 }

val schema = new GenericSchema[ZIOTestService]{}
import schema._

 def buildApi(query: Queries) : GraphQL[Console with Clock with ZIOTestService] =  {
   val api = graphQL(
     RootResolver(
       query
     )
   )
   api
 }




}