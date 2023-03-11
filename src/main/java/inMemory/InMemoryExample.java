package inMemory;

import model.CustomerEntity;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.List;

public class InMemoryExample {

	public static void main(String[] args) {
		String outputPath = "src/main/resources/customer/customer.csv";
		String fileType = ".csv";
		Pipeline pipeline = Pipeline.create();
		PCollection<CustomerEntity> customerEntityPCollection = pipeline.apply(Create.of(getCustomers()));
		PCollection<String> stringPCollection = customerEntityPCollection
				.apply(MapElements.into(TypeDescriptors.strings()).via((CustomerEntity customer) -> customer.getId()
						+ "," + customer.getName() + "," + customer.getBirthDate() + "," + customer.getAge()));
		System.out.println("String pCollection:-" + stringPCollection.toString());
		stringPCollection.apply(TextIO.write().to(outputPath).withNumShards(1).withSuffix(fileType));
		pipeline.run();

	}

	static List<CustomerEntity> getCustomers() {

		CustomerEntity customer = new CustomerEntity("1012", "Harshang", "19", "20-Oct-2003");
		CustomerEntity customer1 = new CustomerEntity("1013", "Manthan", "19", "17-Nov-2003");
		CustomerEntity customer2 = new CustomerEntity("1014", "Rajan", "21", "27-July-2001");

		ArrayList<CustomerEntity> customerEntityList = new ArrayList<>();
		customerEntityList.add(customer);
		customerEntityList.add(customer1);
		customerEntityList.add(customer2);

		return customerEntityList;

	}

}
