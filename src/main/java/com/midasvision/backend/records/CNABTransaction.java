package com.midasvision.backend.records;

import java.math.BigDecimal;

public record CNABTransaction(

        Integer tipo,
        String data,
        BigDecimal valor,
        Long cpf,
        String cartao,
        String hora,
        String donoDaLoja,
        String nomeDaLoja) {

}
