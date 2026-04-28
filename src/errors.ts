export class AbortedRequestError extends Error {
    constructor(url: string) {
        super(`Aborted request for ${url}`);
        this.name = 'AbortedRequestError';
    }
}
