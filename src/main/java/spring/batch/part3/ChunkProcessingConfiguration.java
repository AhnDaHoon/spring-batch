package spring.batch.part3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
@Slf4j
public class ChunkProcessingConfiguration {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    public ChunkProcessingConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    public Job chunkProcessingJob(){
        return jobBuilderFactory.get("chunkProcessingJob")
                .incrementer(new RunIdIncrementer())
                .start(this.taskBaseStep())
                .next(this.chunkBaseStep())
                .build();
    }


    /////////////////////////////// commonMethod ///////////////////////////////

    public List<String> getItems() {
        List<String> items = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            items.add(i + " Hello");
        }

        return items;
    }

    /////////////////////////////// taskBaseStep ///////////////////////////////

    @Bean
    public Step taskBaseStep() {
        return stepBuilderFactory.get("taskBaseStep")
                .tasklet(this.tasklet())
                .build();
    }

//    public Tasklet tasklet() {
//        return ((contribution, chunkContext) -> {
//            List<String> items = getItems();
//            log.info("task item size : {}", items.size());
//
//            return RepeatStatus.FINISHED;
//        });
//    }

    /**
     * tasklet을 chuck처럼 사용하기
     * @return
     */
    public Tasklet tasklet() {
        List<String> items = getItems();
        return ((contribution, chunkContext) -> {
            StepExecution stepExecution = contribution.getStepExecution();
            int chunkSize = 10;
            int fromIndex = stepExecution.getReadCount();
            int toIndex = fromIndex + chunkSize;

            if(fromIndex >= items.size()){
                return RepeatStatus.FINISHED;
            }

            List<String> subList = items.subList(fromIndex, toIndex);
            log.info("task subList size : {}", subList.size());

            stepExecution.setReadCount(toIndex);
            return RepeatStatus.CONTINUABLE;
        });
    }


    /////////////////////////////// chunkBaseStep ///////////////////////////////

    // chunk기반의 step 종료 시점은 ItemReader에서 null을 리턴할 떄 까지 반복한다.
    @Bean
    public Step chunkBaseStep(){
        return stepBuilderFactory.get("chunkBaseStep")

                .<String, String>chunk(10) // ItemReader 에서 읽고 반환되는 input이다 이 input은 ItemProcessor에서 받아서 output타입으로 반환하고 ItemWriter는 List타입을 받게 된다.
                // <ItemReader, ItemProcessor>
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    public ItemWriter< String> itemWriter() {
        return items -> log.info("chunk item size : {}", items.size());
//        return items -> items.forEach(log::info);
    }

    // return으로  null이 넘겨지면 itemWriter가 실행되지 않음
    public ItemProcessor<String, String> itemProcessor() {
        return item -> item + ", Spring Batch";
    }

    public ItemReader<String> itemReader() {
        return new ListItemReader<>(getItems());
    }
}
