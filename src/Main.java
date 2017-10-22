
public class Main {

	public static void main(String[] args) {
		if (args.length < 4) {
			System.err.println("Usage: <class-name> <inputFile> <outputFile> <Iterations>\n"
					+ "   where class-name={PageRank,University,GraphX}");
			System.exit(1);
		}
		String options[] = { args[1], args[2], args[3], "" };
		if (args[0].equals("PageRank"))
			PageRank.main(options);
		else if (args[0].equals("University"))
		{
			options[3] = args[4];
			UniversityPR.main(options);
		}
		else if (args[0].equals("GraphX"))
			GraphX.main(options);
	}

}
