package com.midasvision.backend.configs;

import com.midasvision.backend.records.CNABTransaction;
import com.midasvision.backend.records.Transaction;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class BatchConfig {

    private PlatformTransactionManager manager;
    private JobRepository jobRepo;

    public BatchConfig(PlatformTransactionManager manager, JobRepository jobRepo) {
        this.manager = manager;
        this.jobRepo = jobRepo;
    }

    @Bean
    Job job(Step step) {
        return new JobBuilder("job", jobRepo)
                .start(step)
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    Step step(ItemReader<CNABTransaction> reader, ItemProcessor<CNABTransaction, Transaction> processor, ItemWriter<Transaction> writer) {
        return new StepBuilder("step", jobRepo)
                .<CNABTransaction, Transaction>chunk(1000, manager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    FlatFileItemReader<CNABTransaction> reader() {
        return new FlatFileItemReaderBuilder<CNABTransaction>()
                .name("reader")
                .resource(new FileSystemResource("C:\\Users\\avner\\IdeaProjects\\backend\\files\\CNAB.txt"))
                .fixedLength()
                .columns(
                        new Range(1, 1), new Range(2, 9),
                        new Range(10, 19), new Range(20, 30),
                        new Range(31, 42), new Range(43, 48),
                        new Range(49, 62), new Range(63, 80)
                )
                .names("tipo", "data", "valor", "cpf", "cartao", "hora", "donoDaLoja", "nomeDaLoja")
                .targetType(CNABTransaction.class)
                .build();
    }
}
