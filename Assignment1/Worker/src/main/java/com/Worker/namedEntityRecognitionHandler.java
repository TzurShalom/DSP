package com.Worker;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.endpoints.internal.Value;

public class namedEntityRecognitionHandler
{
    private final StanfordCoreNLP NERPipeline;

    public namedEntityRecognitionHandler()
    {
        Properties props = new Properties();
        props.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        NERPipeline = new StanfordCoreNLP(props);
    }

    public List<String> getEntities(String review)
    {
        List<String> entities = new ArrayList<>();

        // create an empty Annotation just with the given text
        Annotation document = new Annotation(review);
        // run all Annotators on this text
        NERPipeline.annotate(document);
        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        for(CoreMap sentence: sentences)
        {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(TokensAnnotation.class))
            {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);

                if (!ne.equals("O")) { entities.add(word); }

                //System.out.println("\t-" + word + ":" + ne);
            }
        }
        return entities;
    }
}
