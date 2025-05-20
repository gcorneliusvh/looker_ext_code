export interface ICryptoHash {
    secureRandom(byte_count: number): string;
    sha256Hash(message: string): Promise<string>;
}
