
/**
 * Created by dchiu on 9/25/18.
 */
public class DBTester {
    public static void main(String[] args) throws Exception {
        DavidDB db = new DavidDB("data/classicmodels_schema.txt");
		System.out.println(db.toString());		// print schema

		// populate relations
		Relation customers = (Relation) db.getRelation("customers");
		Relation employees = (Relation) db.getRelation("employees");
		Relation offices = (Relation) db.getRelation("offices");
		Relation orderdetails = (Relation) db.getRelation("orderdetails");
		Relation orders = (Relation) db.getRelation("orders");
		Relation payments = (Relation) db.getRelation("payments");
		Relation productlines = (Relation) db.getRelation("productlines");
		Relation products = (Relation) db.getRelation("products");
		customers.read("data/customers.txt");
		employees.read("data/employees.txt");
		offices.read("data/offices.txt");
		orderdetails.read("data/orderdetails.txt");
		orders.read("data/orders.txt");
		payments.read("data/payments.txt");
		productlines.read("data/productlines.txt");
		products.read("data/products.txt");

//		System.out.println(customers);
//		System.out.println(employees);
//		System.out.println(offices);
//		System.out.println(orderdetails);
//		System.out.println(orders);
//		System.out.println(payments);
//		System.out.println(productlines);
//		System.out.println(products);

		System.out.println("========================MY QUERIES");
		Relation nj0 = db.naturalJoin(offices, employees);
		System.out.println(nj0);
		System.out.println("Rows Returned: " + nj0.getTuples().size());
		System.out.println("Elapsed Time: " + db.getElapsedTime() + " ms\n");
		db.resetElapsedTime();
//		Elapsed Time: 628.5571739999999 ms

		Relation hj0 = db.hashJoin(offices, employees);
		System.out.println(hj0);
		System.out.println("Rows Returned: " + hj0.getTuples().size());
		System.out.println("Elapsed Time: " + db.getElapsedTime() + " ms\n");
		db.resetElapsedTime();
//		Elapsed Time: 0.213189 ms
//// TODO and show that employees hashJoin offices doesn't work (uniqueness)


		Relation nj1 = db.naturalJoin(customers, payments);
		System.out.println(nj1);
		System.out.println("Rows Returned: " + nj1.getTuples().size());
		System.out.println("Elapsed Time: " + db.getElapsedTime() + " ms\n");
		db.resetElapsedTime();
//		Elapsed Time: 12225.006272 ms
//
		Relation hj1 = db.hashJoin(customers, payments);
		System.out.println(hj1);
		System.out.println("Rows Returned: " + hj1.getTuples().size());
		System.out.println("Elapsed Time: " + db.getElapsedTime() + " ms\n");
		db.resetElapsedTime();
//		Elapsed Time: 1.850471 ms

		Relation nj2 = db.naturalJoin(customers, employees);
//		System.out.println(nj2);
		System.out.println("Rows Returned: " + nj2.getTuples().size());
		System.out.println("Elapsed Time: " + db.getElapsedTime() + " ms\n");
		db.resetElapsedTime();
////		Elapsed Time: 7.03947 ms
//
		Relation hj2 = db.hashJoin(customers, employees);
//		System.out.println(hj2);
		System.out.println("Rows Returned: " + hj2.getTuples().size());
		System.out.println("Elapsed Time: " + db.getElapsedTime() + " ms\n");
		db.resetElapsedTime();
////		Elapsed Time: 4.66453 ms

		Relation nj3 = db.naturalJoin(productlines, products);
//		System.out.println(nj3);
		System.out.println("Rows Returned: " + nj3.getTuples().size());
		System.out.println("Elapsed Time: " + db.getElapsedTime() + " ms\n");
		db.resetElapsedTime();
//		Elapsed Time: 611.121683 ms

		Relation hj3 = db.hashJoin(productlines, products);
//		System.out.println(hj3);
		System.out.println("Rows Returned: " + hj3.getTuples().size());
		System.out.println("Elapsed Time: " + db.getElapsedTime() + " ms\n");
		db.resetElapsedTime();
//		Elapsed Time: 1.085564 ms

		Relation nj4 = db.naturalJoin(orders,orderdetails);
//		System.out.println(nj4);
		System.out.println("Rows Returned: " + nj4.getTuples().size());
		System.out.println("Elapsed Time: " + db.getElapsedTime() + " ms\n");
		db.resetElapsedTime();
//		Elapsed Time: 397859.53113 ms

		Relation hj4 = db.hashJoin(orders,orderdetails);
//		System.out.println(hj4);
		System.out.println("Rows Returned: " + hj4.getTuples().size());
		System.out.println("Elapsed Time: " + db.getElapsedTime() + " ms\n");
		db.resetElapsedTime();
//		Elapsed Time: 20.387921 ms


	}
}
