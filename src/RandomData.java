import java.util.Random;

public class RandomData {
	public static void main(String[] args) {
		Random random = new Random();

		String[] items = { "book", "car", "cd", "dvd", "hat", "cake" };
		int numberOfTrans = 15;
		int maxNumberOfItemsPerTran = 5;

		for (int i = 0; i < numberOfTrans; i++) {
			int numberOfItems = 5 + random.nextInt(maxNumberOfItemsPerTran);
			for (int j = 0; j < numberOfItems; j++) {
				int randIdx = random.nextInt(items.length);
				String item = items[randIdx];
				int randInt = random.nextInt(5);
				System.out.print(item + randIdx + " ");
			}
			System.out.print("\n");
		}
	}
}
