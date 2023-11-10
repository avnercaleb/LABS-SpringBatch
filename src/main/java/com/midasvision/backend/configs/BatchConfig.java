package com.midasvision.backend.configs;

import com.midasvision.backend.records.CNABTransaction;
import com.midasvision.backend.records.Transaction;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;

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
    @StepScope
    @Bean
    FlatFileItemReader<CNABTransaction> reader(@Value("#{jobParameters['cnabFile']}") Resource resource) {
        return new FlatFileItemReaderBuilder<CNABTransaction>()
                .name("reader")
                .resource(resource)
                .fixedLength()
                .columns(
                        new Range(1, 1), new Range(2, 9),
                        new Range(10, 19), new Range(20, 30),
                        new Range(31, 42), new Range(43, 48),
                        new Range(49, 62), new Range(63, 80)
                )
                .names("tipo", "data", "valor",
                        "cpf", "cartao", "hora",
                        "donoDaLoja", "nomeDaLoja")
                .targetType(CNABTransaction.class)
                .build();
    }

    @Bean
    ItemProcessor<CNABTransaction, Transaction> processor() {
        return item -> {
            Transaction t = new Transaction(
                    null, item.tipo(), null,
                    item.valor().divide(BigDecimal.valueOf(100)),
                    item.cpf(), item.cartao(), null, item.donoDaLoja().trim(),
                    item.nomeDaLoja().trim())
                    .withData(item.data())
                    .withHora(item.hora());

            return t;
        };
    }

    @Bean
    JdbcBatchItemWriter<Transaction> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Transaction>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO transacao (
                            tipo, data, valor, cpf, cartao,
                            hora, dono_loja, nome_loja)
                        VALUES (
                            :tipo, :data, :valor, :cpf, :cartao,
                            :hora, :donoDaLoja, :nomeDaLoja
                        )
                        """)
                .beanMapped()
                .build();
    }

    @Bean
    JobLauncher jobLauncherAsync() throws Exception{

        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepo);
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }
}
