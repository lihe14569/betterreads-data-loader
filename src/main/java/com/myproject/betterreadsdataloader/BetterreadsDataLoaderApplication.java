package com.myproject.betterreadsdataloader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import com.myproject.betterreadsdataloader.author.Author;
import com.myproject.betterreadsdataloader.author.AuthorRepository;
import com.myproject.betterreadsdataloader.book.Book;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import connection.DataStaxAstraProperties;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterreadsDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;

	@Value("${datadump.location.author}")
	private String dataDumpLocation;

	@Value("${datadump.location.works}")
	private String workDumpLocation;

	int writeCounter = 0;

	public static void main(String[] args) {
		SpringApplication.run(BetterreadsDataLoaderApplication.class, args);
	}

	@PostConstruct
	public void start() {
		initAuthors();
		// initWorks();
	}

	private void initWorks() {
		Path path = Paths.get(workDumpLocation);
		try {
			Stream<String> lines = Files.lines(path);
			// read and parse the files
			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					// Create a book object
					Book book = new Book();
					book.setId(jsonObject.optString("key").replace("/works/", ""));
					book.setName(jsonObject.optString("title"));
					book.setDescription(jsonObject.optString("description"));

				} catch (JSONException e) {
					e.printStackTrace();
				}
			});
			// create author objects

			// save the objects in repository
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void initAuthors() {
		Path path = Paths.get(dataDumpLocation);

		try {
			Stream<String> lines = Files.lines(path);
			List<Author> list = new ArrayList<>();

			// read and parse the files
			lines.forEach(line -> {
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					// create author objects
					Author author = new Author();
					author.setName(jsonObject.optString("name").trim());
					author.setPersonalName(jsonObject.optString("personal_name").trim());
					author.setId(jsonObject.optString("key").replace("/a/", "").trim());
					System.out.println("Author Name: " + author.getName() + " -> Added.....");
					list.add(author);
					// save the objects in repository
					if (list.size() >= 1000) {
						authorRepository.saveAll(list);
						writeCounter++;
						System.out.println("Save 1000 authors" + "${writeCounter} times");
						list.clear();
					}
				} catch (JSONException e) {
					e.printStackTrace();
				}

			});

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}
}
